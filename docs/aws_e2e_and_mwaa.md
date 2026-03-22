# AWS end-to-end test and Airflow on MWAA

This guide covers:

1. **Glue + S3 smoke test** — `scripts/e2e_aws.sh`
2. **Optional Amazon MWAA** — Terraform (`enable_mwaa = true`) + DAG sync

## Prerequisites

- AWS CLI configured (`aws sts get-caller-identity`)
- Stack deployed with Terraform (`Glue` job `search-keyword-performance`, data bucket)
- For MWAA: a **VPC** with **at least two private subnets** (different AZs) that can reach AWS APIs (typically **NAT Gateway** or **VPC endpoints**). MWAA is **not** free-tier friendly (environment + NAT + workers).

### Networking gotchas (read before `enable_mwaa=true`)

| Topic | Detail |
|--------|--------|
| **Private subnets only** | MWAA must use **private** subnets. **Do not** place the environment in **public** subnets — creation will fail or behave incorrectly. |
| **Egress to AWS APIs** | Those private subnets need a path to reach **S3**, **Glue**, **CloudWatch**, **STS**, etc. That is usually **NAT Gateway(s)** (per-AZ) and/or **VPC interface endpoints**. Without that, the environment can stay stuck in **CREATING** or tasks fail at runtime. |
| **“Hidden” NAT cost** | **NAT Gateway** hourly + **per-GB data processing** charges are **separate** from the **MWAA environment** hourly rate. For steady workloads, compare NAT spend vs **VPC endpoints** for S3/Glue (often cheaper at scale). |
| **Inbound vs outbound** | The MWAA **security group** often allows **inbound TCP 443** from your `mwaa_ui_allowed_cidrs` (Airflow UI). **RDS/5432** is not opened *inbound* on the MWAA SG for “metadata”: Airflow’s metadata DB is **managed inside MWAA**. If your DAG connects to **your** RDS, that is **outbound** from MWAA workers to the **RDS** security group on **5432** — allow that on the **RDS** side. |

## 1) End-to-end Glue test

From the repo root:

**All-in-one (Glue + Lambda):**

```bash
cp .env.aws.example .env.aws   # edit DATA_BUCKET, AWS_REGION
./scripts/run_aws_e2e.sh
```

**Glue only:**

```bash
chmod +x scripts/e2e_aws.sh
export AWS_REGION=us-west-2
export DATA_BUCKET=your-bucket-name
./scripts/e2e_aws.sh
```

If your Glue job still uses legacy **`input/`** and **`output/`** (instead of Terraform defaults `landing/` and `curated/search_keyword/`), run:

```bash
LANDING_PREFIX=input/ CURATED_PREFIX=output/ ./scripts/e2e_aws.sh
```

The script:

- Uploads `sample_hit_data.tsv` under `<LANDING_PREFIX>/dt=<today UTC>/` (matches Glue partition pruning for `--partition_dt`)
- Starts the Glue job with **`--partition_dt`** = today (UTC) and **`--partition_interval_minutes`** (default `15`)
- Waits until the run **SUCCEEDED**
- Asserts output exists under `<CURATED_PREFIX>/` with a key containing `{today}_SearchKeywordPerformance` (default prefix `curated/search_keyword/`, or `output/` for legacy stacks)

Optional environment variables: `LANDING_PREFIX`, `CURATED_PREFIX`, `GLUE_JOB`, `SAMPLE`, `PARTITION_INTERVAL_MINUTES`.

**Note:** `start-job-run` **merges** `--arguments` with the job’s `default_arguments` in Terraform; you only need overrides like `partition_dt`.

## 2) Deploy Apache Airflow on AWS (MWAA)

**Step-by-step (networking, variables, UI, RDS):** see **`docs/mwaa_deploy.md`**.

This repo wires **optional** [Amazon MWAA](https://docs.aws.amazon.com/mwaa/) in `terraform/mwaa.tf` using the community module [`aws-ia/mwaa/aws`](https://registry.terraform.io/modules/aws-ia/mwaa/aws) (version pinned in `mwaa.tf`). It stores DAGs in the **same S3 data bucket** under `airflow/dags/` and uploads **`airflow/requirements.txt`** (e.g. `pg8000` for DB verification) for the MWAA environment.

### 2.1 Terraform variables

Set when applying:

| Variable | Description |
|----------|-------------|
| `enable_mwaa` | `true` to create MWAA |
| `mwaa_vpc_id` | VPC for MWAA |
| `mwaa_private_subnet_ids` | **≥ 2** private subnet IDs |
| `mwaa_environment_name` | Unique environment name |
| `mwaa_webserver_access_mode` | `PUBLIC_ONLY` (internet UI) or `PRIVATE_ONLY` (VPN/VPC) |
| `mwaa_ui_allowed_cidrs` | CIDRs allowed to HTTPS (443) on the MWAA security group (e.g. corporate VPN); default in Terraform is open for dev — **tighten for production** |
| `mwaa_airflow_version` | Must be [supported in your region](https://docs.aws.amazon.com/mwaa/latest/userguide/airflow-versions.html) |

Example (adjust names):

```bash
cd terraform
terraform apply \
  -var="bucket_name=your-bucket-name" \
  -var="enable_mwaa=true" \
  -var="mwaa_vpc_id=vpc-xxxxxxxx" \
  -var='mwaa_private_subnet_ids=["subnet-aaaa","subnet-bbbb"]' \
  -var="mwaa_webserver_access_mode=PUBLIC_ONLY"
```

Outputs include `mwaa_webserver_url` and `mwaa_dag_s3_uri`.

### 2.2 Upload DAGs to S3

**Pro tip:** Creating or updating an MWAA environment often takes **~20–30 minutes**. Use that window to **sync DAGs** and **pre-fill Airflow Variables** (below) so you are not idle after the UI URL appears.

After MWAA is **AVAILABLE**, sync DAGs (same prefix as `mwaa_dag_s3_path`, default `airflow/dags`):

```bash
export DATA_BUCKET=your-bucket-name
# Optional: export MWAA_DAGS_PREFIX=airflow/dags
chmod +x scripts/sync_airflow_dags_to_s3.sh
./scripts/sync_airflow_dags_to_s3.sh
```

Using `scripts/sync_airflow_dags_to_s3.sh` is preferable to one-off console uploads — repeatable and CI-friendly.

MWAA usually picks up changes within a few minutes.

### 2.3 Airflow Variables and connection

In the MWAA UI (**Admin → Variables**):

| Variable | Purpose |
|----------|---------|
| `search_keyword_bucket` | S3 data bucket name (Glue + verification tasks) — **required** for the S3 verify task (DAG defaults to empty if unset and fails with a clear error). |
| `sync_db_sinks` | `true` / `false` — must match whether Glue should write to RDS |
| `db_host`, `db_port`, `db_name`, `db_secret_arn`, `db_fact_table`, `db_ai_table` | If using DB sinks / DAG DB verification |
| `db_verify_mode` | `auto` or `strict` when `sync_db_sinks=true` |
| `airflow_invoke_lambda` | `true` / `false` (default **`true`**) — set **`false`** to skip the optional **Lambda smoke** task (`invoke_lambda_smoke`) if you only want Glue + S3 + DB checks. |
| `lambda_function_name` | Lambda name to invoke (default **`search-keyword-performance`**) — must match Terraform. |
| `aws_default_region` | Optional — if set, boto3 in the Lambda task uses this region; otherwise the default credential chain applies. |

**DAG order:** `start_glue` → wait → **S3 verify** → **DB verify** (when `sync_db_sinks`) → **Lambda smoke** (when `airflow_invoke_lambda`). DB verification runs **before** Lambda so a failing Lambda (e.g. DB/pyarrow) does not block checking Glue’s Postgres writes.

**Admin → Connections:** ensure **`aws_default`** uses the correct AWS region (execution role already allows Glue/S3; the DAG uses provider operators).

### 2.4 IAM note (least privilege “extra” policy)

Terraform attaches **`${bucket_name}-mwaa-airflow-extra`** to the **MWAA execution role**. That matches what the DAG actually does:

| Need | Why |
|------|-----|
| **Glue** — `StartJobRun`, `GetJobRun`, `GetJobRuns`, … on job **`search-keyword-performance`** | `GlueJobOperator` / sensors trigger and poll the job. The job name must match **exactly** (Terraform default). |
| **S3** — list/get (and related) on the **data bucket** | DAG tasks list objects under **landing/curated** prefixes for `verify_output_exists`. |
| **Lambda** — `InvokeFunction` on **`search-keyword-performance`** | Only when **`enable_lambda = true`** in Terraform — for the optional `invoke_lambda_smoke` DAG task. |
| **Secrets Manager** — optional | Only if `db_secret_arn` is set in Terraform — for DAG-side DB verification when `sync_db_sinks` is used. |

If you add new AWS calls from the DAG, extend IAM accordingly.

### 2.5 Pre-flight checklist

| Item | Status | If it fails |
|------|--------|-------------|
| **Private subnets** (≥ 2 AZs) with **egress** (NAT or endpoints) | Required | Fix routes / add NAT or S3/Glue/VPC endpoints. |
| **IAM policy** attached | Required | Confirm policy name suffix `-mwaa-airflow-extra`; Glue job name **`search-keyword-performance`**. |
| **Security groups** | Required | **Inbound 443** from allowed CIDRs for UI (when using module rules); **RDS**: allow **from MWAA SG → RDS SG on 5432** (outbound from MWAA, not inbound 5432 on MWAA for metadata). |
| **DAGs in S3** | Pending first deploy | Run `sync_airflow_dags_to_s3.sh` after environment is available. |
| **Airflow Variables + `aws_default`** | Pending | Set `search_keyword_bucket`, Glue/DB vars as needed; **Admin → Connections → `aws_default`** region. |

## 3) Troubleshooting

| Symptom | What to check |
|---------|----------------|
| MWAA stuck **CREATING** | Private subnets need NAT or endpoints; check MWAA service quotas |
| DAG import errors (`amazon` provider) | MWAA includes Airflow; ensure DAG matches the Airflow 2.x API |
| Glue permission denied | Execution role policy + Glue job name `search-keyword-performance` |
| No curated output in E2E | Glue logs; `partition_dt` vs landing paths; `partition_interval_minutes` vs Terraform |

## 4) Costs (summary)

- **MWAA** — environment class + schedulers/workers (hourly).
- **NAT Gateway** — **separate** hourly + per-GB charges (often the surprise line item next to MWAA).
- **Glue** — DPU-time.
- **S3** — storage and requests.

Use `PUBLIC_ONLY` + open `0.0.0.0/0` only for short-lived dev environments.
