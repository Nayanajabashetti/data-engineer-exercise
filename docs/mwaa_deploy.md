# Run Apache Airflow on AWS (Amazon MWAA)

**Console-first runbook (list MWAA bucket, sync DAGs, bucket Variable/SSM, trigger):** **`docs/mwaa_ship_and_run.md`**.  
Optional: **`./scripts/verify_mwaa_ready.sh`** then **`./scripts/sync_airflow_dags_to_s3.sh`**, or both in **`./scripts/mwaa_preflight_and_sync.sh`** (set **`MWAA_ENV_NAME`**, **`DATA_BUCKET`**, **`AWS_REGION`**).

This repo can deploy **Amazon MWAA** (Managed Workflows for Apache Airflow) via Terraform (`terraform/mwaa.tf`). The environment uses your **data bucket** for:

| S3 key | Purpose |
|--------|---------|
| `airflow/dags/` | DAG files — sync from `airflow/dags/` in the repo |
| `airflow/requirements.txt` | Extra Python packages (e.g. `pg8000` for DB verification) — managed by Terraform |

## Prerequisites

1. **AWS account** and credentials (`aws sts get-caller-identity`).
2. **Networking for MWAA** — pick one:
   - **Recommended:** set **`create_mwaa_network = true`** in Terraform. This creates a dedicated **VPC** (`10.42.0.0/16` by default), **two private subnets** (two AZs), one **public** subnet, **Internet Gateway**, and a **single NAT Gateway** (adds ~NAT hourly + EIP cost; fine for dev).
   - **Or** bring your own **VPC ID** + **two private subnet IDs** (`create_mwaa_network = false`). **Do not** use the **default VPC’s public subnets** in the MWAA console — MWAA requires **private** subnets with egress (NAT or interface endpoints).
3. **Outbound internet** from private subnets (NAT and/or **VPC interface endpoints** for MWAA/S3/Glue/STS/CloudWatch/etc.). Without egress, the environment can stay in **CREATING** or tasks fail at runtime.
4. **Terraform** ≥ 1.5 applied at least once for the **S3 bucket + Glue + Lambda** stack (same `bucket_name`).

## 1. Enable MWAA in Terraform

From `terraform/`:

**Option A — Terraform creates the VPC (recommended):**

```bash
terraform apply \
  -var="bucket_name=YOUR_BUCKET" \
  -var="enable_mwaa=true" \
  -var="enable_lambda=true" \
  -var="create_mwaa_network=true" \
  -var="mwaa_webserver_access_mode=PUBLIC_ONLY"
```

**Option B — existing private subnets:**

```bash
terraform apply \
  -var="bucket_name=YOUR_BUCKET" \
  -var="enable_mwaa=true" \
  -var="enable_lambda=true" \
  -var="mwaa_vpc_id=vpc-xxxxxxxx" \
  -var='mwaa_private_subnet_ids=["subnet-aaaa","subnet-bbbb"]' \
  -var="mwaa_webserver_access_mode=PUBLIC_ONLY"
```

Adjust:

- `mwaa_ui_allowed_cidrs` — default `["0.0.0.0/0"]` is for dev only; **tighten** for production (e.g. corporate VPN CIDR).
- `mwaa_airflow_version` — must be [supported in your region](https://docs.aws.amazon.com/mwaa/latest/userguide/airflow-versions.html).

Wait until the environment status is **AVAILABLE** (often **20–40 minutes**).

Get the UI URL:

```bash
terraform output -raw mwaa_webserver_url
terraform output -raw mwaa_dag_s3_uri
terraform output -raw mwaa_requirements_s3_uri
```

## 2. Sync DAGs to S3

DAGs are **not** deployed automatically from Git — upload them to the bucket prefix MWAA uses (default `airflow/dags/`):

```bash
cd /path/to/search-keyword-performance
export DATA_BUCKET=YOUR_BUCKET
chmod +x scripts/sync_airflow_dags_to_s3.sh scripts/deploy_mwaa.sh
./scripts/sync_airflow_dags_to_s3.sh
```

Or use `./scripts/deploy_mwaa.sh` (syncs DAGs and prints reminders).

MWAA usually picks up new DAG files within a few minutes.

## 3. Airflow UI configuration

Open **`mwaa_webserver_url`** in a browser.

### Admin → Variables

| Variable | Purpose |
|----------|---------|
| `search_keyword_bucket` | **Optional** if Terraform created SSM `mwaa_data_bucket_ssm_parameter_name` (default `/search-keyword-performance/airflow/data_bucket_name`) — the DAG reads the bucket from SSM automatically. Set this Variable to override. |
| `sync_db_sinks` | `true` / `false` — must match whether Glue/Lambda write to RDS. |
| `airflow_invoke_lambda` | `true` (default) or `false` to skip Lambda smoke task. |
| **DB variables** | If `sync_db_sinks=true`: `db_host`, `db_port`, `db_name`, `db_secret_arn`, `db_fact_table`, `db_ai_table`, `db_verify_mode`. |

### Admin → Connections

- **`aws_default`** — set **Extra** to your region, e.g. `{"region_name": "us-west-2"}` (MWAA execution role already has AWS access; this sets the region for boto3/operators).

### RDS (optional)

If **`verify_db_sinks_e2e`** must reach **RDS in a VPC**:

1. Allow **outbound** from the MWAA **workers** to the database: on the **RDS** security group, allow **inbound TCP 5432** from the MWAA security group (see `terraform output mwaa_security_group_id`) or from the MWAA VPC CIDR, depending on your layout.
2. `pg8000` is installed via **`airflow/requirements.txt`** on MWAA; after changing that file, run **`terraform apply`** and **update the MWAA environment** (or wait for the next maintenance window) so workers pick up new packages.

## 4. Run the DAG

- DAG id: **`search_keyword_glue_pipeline`**
- Trigger it from the UI (▶) or the Airflow CLI/API.

## 5. Updating `requirements.txt`

1. Edit `airflow/requirements.txt` in the repo.
2. Run **`terraform apply`** (uploads a new object version to S3).
3. In the **MWAA console** → your environment → **Edit** → save to trigger an **update** (installs new requirements). The module may ignore version drift in Terraform; if the environment does not refresh, use **Update environment** in AWS.

## Troubleshooting

| Symptom | What to check |
|---------|----------------|
| **`SubscriptionRequiredException`** (*needs a subscription for the service*) | **Account billing:** MWAA is a **paid** service. Add/verify a **default payment method** in **AWS Billing**; new accounts must finish activation. If it persists, contact **AWS Support**. Workaround: apply with **`enable_mwaa=false`** and run Glue/Lambda via **`./scripts/run_aws_e2e.sh`** or the Glue console (no Airflow). |
| Environment stuck **CREATING** | Private subnets, NAT or VPC endpoints, IAM/service-linked roles. |
| DAG import errors | Provider packages — MWAA includes `apache-airflow-providers-amazon`; extra deps go in `airflow/requirements.txt`. |
| Bucket / S3 errors | Set Variable `search_keyword_bucket` or ensure SSM parameter exists (Terraform `enable_mwaa`). |
| Glue **AccessDenied** | IAM policy `*-mwaa-airflow-extra` attached; Glue job name `search-keyword-performance`. |
| Lambda task fails | `enable_lambda=true` in Terraform; Variable `airflow_invoke_lambda`, function name. |
| DB verification fails | `sync_db_sinks` and DB vars; security groups; `pg8000` installed (requirements). |

## Cost note

MWAA charges for the environment class, workers, and metadata; **NAT Gateway** is often a separate significant line item. Use `PUBLIC_ONLY` + open `0.0.0.0/0` only for short-lived dev environments.
