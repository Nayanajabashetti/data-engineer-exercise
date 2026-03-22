# Exact paths & commands: Glue, Lambda, MWAA (Airflow on AWS)

Replace placeholders:

| Placeholder | Your value (example) |
|-------------|----------------------|
| `REPO` | Path to this repo, e.g. `~/Desktop/search-keyword-performance` |
| `BUCKET` | S3 data bucket, e.g. `acs-keyword-revenue-nayanaj` |
| `REGION` | e.g. `us-west-2` |
| `VPC` / `SUBNET_A` / `SUBNET_B` | For MWAA only — private subnets in **different AZs** |

---

## A. One-time: Terraform (data bucket, Glue, Lambda)

```bash
cd REPO/terraform
terraform init
terraform apply -var="bucket_name=BUCKET"
```

- Approve with **`yes`** when prompted.
- **Do not** paste `bucket_name = "..."` at the shell — use `-var=...` or `terraform.tfvars` (see `terraform/terraform.tfvars.example`).

**Outputs to remember:**

```bash
terraform output -raw s3_bucket_name
terraform output -raw s3_landing_prefix
terraform output -raw s3_curated_prefix
```

---

## B. Glue + Lambda end-to-end (no Airflow)

From **`REPO`** (repo root, **not** `terraform/`):

```bash
cd REPO
cp .env.aws.example .env.aws
```

Edit **`REPO/.env.aws`**:

```bash
AWS_REGION=REGION
DATA_BUCKET=BUCKET
LANDING_PREFIX=landing/
CURATED_PREFIX=curated/search_keyword/
INPUT_PREFIX=landing/
```

Run:

```bash
chmod +x scripts/run_aws_e2e.sh scripts/e2e_aws.sh scripts/lambda_timing_aws.sh
./scripts/run_aws_e2e.sh
```

- **Step 1:** Glue — uploads `REPO/sample_hit_data.tsv` to  
  `s3://BUCKET/landing/dt=<UTC-date>/e2e_<timestamp>_sample_hit_data.tsv`, runs job `search-keyword-performance`, checks **curated** output under  
  `s3://BUCKET/curated/search_keyword/`.
- **Step 2:** Lambda — uploads under `landing/`, invokes Lambda, prints **REPORT** (Duration ms).

**Faster options:**

```bash
SKIP_GLUE=1 ./scripts/run_aws_e2e.sh    # Lambda only
SKIP_LAMBDA=1 ./scripts/run_aws_e2e.sh  # Glue only
```

**Without `.env.aws`:**

```bash
export AWS_REGION=REGION
export DATA_BUCKET=BUCKET
export LANDING_PREFIX=landing/
export CURATED_PREFIX=curated/search_keyword/
export INPUT_PREFIX=landing/
./scripts/e2e_aws.sh
./scripts/lambda_timing_aws.sh
```

---

## C. Airflow on AWS (Amazon MWAA) — optional

Requires **VPC + ≥ 2 private subnets** (NAT or VPC endpoints for egress). **Costs** apply (MWAA + often NAT).

### C1. Enable MWAA in Terraform

```bash
cd REPO/terraform
terraform apply \
  -var="bucket_name=BUCKET" \
  -var="enable_mwaa=true" \
  -var="mwaa_vpc_id=VPC_ID" \
  -var='mwaa_private_subnet_ids=["SUBNET_A","SUBNET_B"]' \
  -var="mwaa_webserver_access_mode=PUBLIC_ONLY"
```

- Wait until environment status is **AVAILABLE** (often **20–30+ minutes**).
- Get UI URL:

```bash
terraform output -raw mwaa_webserver_url
terraform output -raw mwaa_dag_s3_uri
```

### C2. Sync DAGs to S3

DAGs live in **`REPO/airflow/dags/`**. MWAA reads them from **`s3://BUCKET/airflow/dags/`** (default).

```bash
cd REPO
export DATA_BUCKET=BUCKET
export MWAA_DAGS_PREFIX=airflow/dags
chmod +x scripts/sync_airflow_dags_to_s3.sh
./scripts/sync_airflow_dags_to_s3.sh
```

### C3. Airflow UI (browser)

1. Open **`mwaa_webserver_url`** from Terraform output.
2. **Admin → Variables** — set at least:

| Key | Value |
|-----|--------|
| `search_keyword_bucket` | `BUCKET` |
| `sync_db_sinks` | `false` or `true` (match Glue/Lambda DB config) |
| `airflow_invoke_lambda` | `true` (default) or `false` to skip the Lambda smoke task |
| Optional DB vars | If using DB verification |

3. **Admin → Connections** — **`aws_default`**: region = `REGION` (uses MWAA execution role for AWS).

**DAG flow:** Glue → S3 verify → DB verify (if enabled) → Lambda smoke (if `airflow_invoke_lambda` is true). Terraform must have **`enable_lambda = true`** if you want the MWAA role to invoke Lambda.

### C4. Run the DAG

- DAG id: **`search_keyword_glue_pipeline`**
- Trigger from the UI (play button) or CLI using MWAA / Airflow API per your org.

---

## D. Path reference (defaults after Terraform)

| What | S3 path |
|------|---------|
| Lambda / Glue **landing** (trigger + read) | `s3://BUCKET/landing/` |
| Glue / Lambda **curated** output | `s3://BUCKET/curated/search_keyword/` |
| Optional staging (Glue) | `s3://BUCKET/staging/search_hits/` |
| MWAA DAGs | `s3://BUCKET/airflow/dags/` |
| Glue script (deployed by Terraform) | `s3://BUCKET/scripts/glue_job.py` |

---

## E. Troubleshooting

| Issue | Check |
|-------|--------|
| `your-bucket-name` / AccessDenied on upload | **`REPO/.env.aws`** — `DATA_BUCKET=BUCKET` (real name). |
| Terraform `prevent_destroy` on bucket | **`bucket_name`** must match existing state (`terraform state show aws_s3_bucket.data`). |
| zsh `!` in secret ARN | Use **single-quoted** `-var='db_secret_arn=arn:...secret:rds!...'` |
| MWAA stuck | Private subnets + NAT / endpoints |

More detail: **`docs/aws_e2e_and_mwaa.md`**, **`docs/glue_runtime_latency.md`**.
