# Run the pipeline from the Airflow UI only

Goal: open **Amazon MWAA** in the browser, click **Trigger** on one DAG, and run **Glue → S3 check → DB check (optional) → Lambda smoke (optional)** without using the CLI for each run.

## One-time (before you can “just click Run”)

1. **Deploy AWS with Terraform** — same bucket, Glue job, Lambda, and **MWAA** (`enable_mwaa=true`, VPC + 2× private subnets).  
   Wait until MWAA status is **AVAILABLE**.

2. **Apply Terraform** (stack + MWAA + SSM bucket hint). From repo root with `.env.aws` or `terraform/terraform.tfvars`:

   ```bash
   TF_APPLY_AUTO_APPROVE=1 ./scripts/terraform_apply.sh apply
   ```

   **Apply + sync DAGs in one go:**

   ```bash
   SYNC_AIRFLOW_DAGS=1 TF_APPLY_AUTO_APPROVE=1 ./scripts/deploy_aws.sh
   ```

3. **Sync DAGs only** (if you skipped `SYNC_AIRFLOW_DAGS=1` above):

   ```bash
   export DATA_BUCKET=YOUR_BUCKET
   ./scripts/sync_airflow_dags_to_s3.sh
   ```

   Or: `./scripts/deploy_mwaa.sh`

4. **SSM + DAG** — Terraform + MWAA create an **SSM parameter** with your data bucket name (`/search-keyword-performance/airflow/data_bucket_name` by default).  
   The DAG reads that automatically so you **do not need** to set Airflow Variable `search_keyword_bucket` unless you want to override it.

## Every time you want an end-to-end run

1. Open the **MWAA web server URL** (from Terraform: `terraform output -raw mwaa_webserver_url`, or **AWS Console → MWAA → Environments → Open Airflow UI**).

2. Turn **ON** the DAG **`search_keyword_glue_pipeline`** (toggle on the left).

3. Click **Trigger** ( ▶ ) on that DAG.

That’s it for the **default** path: Glue runs, S3 output is verified, optional DB/Lambda steps follow your Variables (see below).

### Optional Airflow Variables (only if you need them)

| Variable | When |
|----------|------|
| `search_keyword_bucket` | Override the bucket name (otherwise SSM + Terraform are used). |
| `sync_db_sinks` | Set `true` only if Glue writes to RDS **and** you want the DB verification task to run (needs DB Variables). |
| `airflow_invoke_lambda` | Set `false` to skip the Lambda smoke task. |
| `aws_default_region` | If boto3 clients need an explicit region (rare on MWAA). |

### Connection `aws_default`

MWAA usually provides **`aws_default`** with the execution role. If Glue tasks fail with “connection” or region errors, open **Admin → Connections → aws_default** and set **Extra** to your region, e.g. `{"region_name": "us-west-2"}`.

## What you are not expected to do every run

- No `terraform apply` per run.  
- No `aws glue start-job-run` from the CLI.  
- No need to set `search_keyword_bucket` if SSM was created by Terraform.

You only **sync DAGs** again when you change files under `airflow/dags/`.

More detail: **`docs/mwaa_deploy.md`**.
