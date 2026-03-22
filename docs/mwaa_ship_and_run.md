# MWAA: confirm identity → ship DAGs → verify SSM → trigger

Target bucket in examples: **`acs-keyword-revenue-nayanaj`**, region **`us-west-2`**. Adjust if yours differ.

---

## Configure → preflight & sync → run (shortest path)

**1. Configure** (replace `your-mwaa-environment-name` with output of `aws mwaa list-environments`):

```bash
export MWAA_ENV_NAME=your-mwaa-environment-name
export DATA_BUCKET=acs-keyword-revenue-nayanaj
export AWS_REGION=us-west-2
```

Template: **`mwaa.env.example`** in the repo root.

**2. Execute preflight & sync** (validator + upload DAGs to the MWAA bucket):

```bash
./scripts/mwaa_preflight_and_sync.sh
```

**3. Deploy & run** — Open the **MWAA** console → **Open Airflow UI** → DAG **`search_keyword_glue_pipeline`** → Unpause → **Trigger**.

---

## Recommended order (validator → ship → trigger)

**1. Run the validator** — catches IAM / wrong region / missing SSM **before** you upload DAGs:

```bash
export MWAA_ENV_NAME=your-mwaa-environment-name
export AWS_REGION=us-west-2
./scripts/verify_mwaa_ready.sh
```

**2. Ship to S3** — mirror local `airflow/dags/` into the MWAA source bucket:

```bash
export DATA_BUCKET=acs-keyword-revenue-nayanaj
./scripts/sync_airflow_dags_to_s3.sh
```

**Or one command** (validator + sync):

```bash
export MWAA_ENV_NAME=your-mwaa-environment-name
export DATA_BUCKET=acs-keyword-revenue-nayanaj
export AWS_REGION=us-west-2
./scripts/mwaa_preflight_and_sync.sh
```

**3. “Silence is golden” (skip Airflow Variables)** — If `verify_mwaa_ready.sh` successfully read SSM parameter **`/search-keyword-performance/airflow/data_bucket_name`**, you do **not** need **Admin → Variables → `search_keyword_bucket`**. Leave it unset: the DAG **falls back** to that SSM value after Variable and env are empty.

**4. Trigger** — MWAA UI → DAG **`search_keyword_glue_pipeline`** → Unpause → **Trigger**.

---

## How the DAG resolves the **data** bucket

`search_keyword_pipeline_dag.py` uses the **first** non-empty value in this order:

1. Airflow Variable **`search_keyword_bucket`**
2. Environment variable **`SEARCH_KEYWORD_DATA_BUCKET`** (optional)
3. SSM **`/search-keyword-performance/airflow/data_bucket_name`** (created by Terraform when **`enable_mwaa=true`**)

So SSM is the **hands-off** path when you **do not** set the Variable — not “SSM first” in code, but in practice you can leave the Variable empty and rely on SSM alone.

---

## 1. Confirm MWAA identity & target

Run as a user/role that can read MWAA (e.g. **terraform-admin** or equivalent). Replace **`YOUR_ENV_NAME`** (from `list-environments`).

```bash
aws mwaa list-environments --region us-west-2

aws mwaa get-environment --name YOUR_ENV_NAME --region us-west-2 \
  --query 'Environment.{SourceBucketArn:SourceBucketArn,DagS3Path:DagS3Path}' --output table
```

**Expected with repo defaults:** `SourceBucketArn` ends with **`acs-keyword-revenue-nayanaj`** and **`DagS3Path`** is **`airflow/dags`**, so DAGs load from:

`s3://acs-keyword-revenue-nayanaj/airflow/dags/`

If your table shows a different bucket or prefix, use that in step 2 (`DATA_BUCKET` / `MWAA_DAGS_PREFIX`).

---

## 2. Ship the code (keep folder layout)

Do **not** rely on one-off `cp` of a single file; sync the whole DAG folder.

```bash
cd /path/to/search-keyword-performance
export DATA_BUCKET=acs-keyword-revenue-nayanaj
export MWAA_DAGS_PREFIX=airflow/dags   # must match MWAA DagS3Path from step 1
chmod +x ./scripts/sync_airflow_dags_to_s3.sh
./scripts/sync_airflow_dags_to_s3.sh
```

This uploads everything under **`airflow/dags/`** (including `lambda_smoke_sample.parquet` next to the DAG).

---

## 3. Verify the “source of truth” (SSM vs Variable)

**SSM (no Airflow Admin Variables needed)** — if Terraform created the parameter:

```bash
aws ssm get-parameter --name "/search-keyword-performance/airflow/data_bucket_name" --region us-west-2
```

You should see **`acs-keyword-revenue-nayanaj`** (or your data bucket) in **`Parameter.Value`**.  
Then leave **`search_keyword_bucket`** unset in Airflow; the DAG will use SSM after Variable/env are empty.

If SSM is **missing** (MWAA not deployed via this repo’s Terraform, or `enable_mwaa=false`), set **Admin → Variables → `search_keyword_bucket`** = **`acs-keyword-revenue-nayanaj`**.

---

## 4. Trigger the pipeline

1. AWS Console → **MWAA** → your environment → **Open Airflow UI**.
2. DAG **`search_keyword_glue_pipeline`** → **Unpause** (toggle on).
3. **Trigger DAG** and watch **Graph** / task logs.
4. Confirm tasks resolve the bucket (S3 verify should list **`curated/search_keyword/`** under your bucket).

If Glue fails on region, set **Admin → Connections → `aws_default`** → Extra: `{"region_name": "us-west-2"}`.

---

## Quick reference

| Step | Command / action |
|------|-------------------|
| 1 | `get-environment` → bucket + `DagS3Path` |
| 2 | `sync_airflow_dags_to_s3.sh` with matching `DATA_BUCKET` / prefix |
| 3 | `aws ssm get-parameter ...` **or** Variable `search_keyword_bucket` |
| 4 | Unpause DAG → Trigger |

More: **`docs/mwaa_deploy.md`**, **`docs/airflow_ui_quickstart.md`**.
