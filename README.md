# data-engineer-exercise

Search Keyword Performance Analyzer: a Python application that parses Adobe Analytics hit-level data to determine how much revenue is generated from external search engines and which keywords perform best. Includes local CLI mode and an AWS Glue (Spark) path for scaling to 10GB+ datasets.

A Python application that parses Adobe Analytics hit-level data to determine how much revenue is generated from external search engines and which keywords perform best.

## Business Question

1. **How much revenue is generated from external search engines?** (Google, Bing, Yahoo, etc.)
2. **Which specific keywords drive the most revenue?**

## Architecture

```
                         ┌─────────────────────────────────────────┐
                         │            AWS Cloud                    │
                         │                                        │
┌──────────┐   upload    │  ┌──────────┐  trigger  ┌───────────┐  │  ┌──────────┐
│ Hit-level │───────────▶│  │  S3      │─────────▶│ Glue Job  │──│─▶│  S3      │
│ TSV file  │            │  │  input/  │          │ (Spark)   │  │  │  output/ │
└──────────┘             │  └──────────┘          └───────────┘  │  └──────────┘
                         └─────────────────────────────────────────┘

      OR (local development)

┌──────────┐             ┌──────────────────────┐            ┌──────────┐
│ TSV file │────────────▶│ SearchKeywordAnalyzer │───────────▶│ .tab     │
└──────────┘   CLI       │ (Python class)        │            │ output   │
                         └──────────────────────┘            └──────────┘
```

**Local mode**: CLI accepts a file path, writes output to `./output/`.

**AWS mode**: Glue ETL job reads from S3, processes with Spark across multiple workers, writes results back to S3. Handles files of any size.

**Lambda mode**: S3 upload to `input/` triggers Lambda for lightweight processing and writes a `.parquet` file to `output/`.

### Attribution Model

For each visitor (tracked by IP + User-Agent composite key), the application remembers the most recent external search-engine referrer. When a purchase event (`event_list` contains `1`) fires, the revenue from `product_list` is attributed to that search engine and keyword. Keywords are normalized to lowercase for accurate aggregation.

### Why Glue over Lambda?

| Concern | Lambda | Glue |
|---|---|---|
| **Timeout** | 15 min max | No limit |
| **Storage** | 512 MB /tmp (10 GB max) | Distributed across workers |
| **Parallelism** | Single-threaded | Spark partitions across N workers |
| **10 GB+ files** | Fails or requires complex chunking | Native -- just add workers |

## Quick Start

### Prerequisites

- Python 3.10+

### Install & Run

```bash
pip install -r requirements.txt
python -m src.main /path/to/data.tsv
```

Output is written to `./output/YYYY-mm-dd_SearchKeywordPerformance.tab`.

### Run Tests

```bash
pytest tests/ -v
```

## AWS Deployment (Terraform)

### Deploy

```bash
cd terraform
terraform init
terraform plan -var="bucket_name=my-search-keyword-data"
terraform apply -var="bucket_name=my-search-keyword-data"
```

This creates:
- An S3 bucket for input/output data
- A Glue ETL job (`glue_job.py` uploaded to S3)
- An IAM role with least-privilege S3 + Glue permissions
- An optional S3-triggered Lambda function (`src/lambda_handler.py`)

### Store secrets in SSM Parameter Store (SecureString)

Create a SecureString parameter in AWS Systems Manager Parameter Store (recommended) and keep the secret value out of source control and Terraform state.

Default parameter name expected by Lambda:
`/search-keyword-performance/api-key`

### Run the Glue Job

```bash
# Upload input data
aws s3 cp data.tsv s3://my-search-keyword-data/input/data.tsv

# Trigger the job
aws glue start-job-run --job-name search-keyword-performance
```

Results appear in `s3://my-search-keyword-data/output/`.

### Run the Lambda Path

After Terraform deploys Lambda, uploading a file to `input/` triggers processing automatically:

```bash
aws s3 cp data.tsv s3://my-search-keyword-data/input/data.tsv
aws s3 ls s3://my-search-keyword-data/output/ --recursive
```

The Lambda path writes a Parquet file directly to:
`s3://my-search-keyword-data/output/<input-stem>_YYYY-mm-dd_SearchKeywordPerformance.parquet`

## Airflow Orchestration

An example Airflow DAG is provided at:
`airflow/dags/search_keyword_pipeline_dag.py`

It performs:
1. Trigger Glue job
2. Wait for Glue completion
3. Verify output exists in S3
4. Optionally verify DB sinks (`sync_db_sinks=true`; use Airflow Variable `db_verify_mode=auto` when Airflow cannot reach private RDS from your laptop)

### Airflow dependencies

Install Amazon provider in your Airflow environment:

```bash
pip install apache-airflow-providers-amazon
```

Configure Airflow connection `aws_default` with AWS credentials/region, then trigger DAG `search_keyword_glue_pipeline`.

### Optional DB sink configuration (Redshift + Aurora)

Both Lambda and Glue can optionally write aggregated outputs to PostgreSQL (RDS):
- BI fact table (`fact_keyword_performance` by default)
- AI insights table (`ai_keyword_insights` by default)

Terraform variables (defaults keep this disabled):
- `enable_db_sinks`
- `db_host`, `db_port`, `db_name`, `db_secret_arn`
- `db_fact_table`, `db_ai_table`

Airflow DAG variables (same names as above) are passed to `GlueJobOperator` script args.
Set `sync_db_sinks=true` in Airflow Variables to enable DB writes from Glue runs.

### Live demo checklist (end-to-end)

1. **Terraform** — deploy with your real `bucket_name` and, if you use RDS sinks, `enable_db_sinks=true` plus `db_*` variables (see below).
2. **Glue** — job already pulls `pg8000` via `--additional-python-modules` (see `terraform/main.tf`).
3. **Lambda + Postgres** — the deployment zip is **source-only** (`src/`). For DB sync, Lambda needs **`pg8000`** at runtime:
   ```bash
   chmod +x scripts/build_lambda_pg_layer.sh
   ./scripts/build_lambda_pg_layer.sh
   aws lambda publish-layer-version --layer-name search-keyword-pg8000 \
     --zip-file fileb://.lambda-pg-layer.zip --compatible-runtimes python3.12 --region us-west-2
   aws lambda update-function-configuration --function-name search-keyword-performance \
     --layers <LayerVersionArnFromPreviousCommand> --region us-west-2
   ```
4. **Lambda + Parquet (optional)** — attach a **pyarrow** layer or bundle `pyarrow` if you want Parquet on the Lambda path (otherwise it falls back to `.tab`).
5. **Smoke test** — `aws s3 cp sample_hit_data.tsv s3://<bucket>/input/demo.tsv` then check CloudWatch and `s3://<bucket>/output/`.
6. **Airflow (local)** — see Docker section below; set **Admin → Variables** (`search_keyword_bucket`, `sync_db_sinks`, `db_*`, and for laptops without RDS reachability use `db_verify_mode=auto`).

### Airflow UI via Docker (recommended for local stability)

If native Airflow webserver crashes locally on macOS, run the UI with Docker:

```bash
cd airflow
# Use 50000 unless you know your host UID exists in the container (macOS UIDs often break Airflow 2.9+).
echo "AIRFLOW_UID=50000" > .env
docker compose up airflow-init
docker compose up -d
```

Open:
`http://localhost:8080`

Default login:
- username: `admin`
- password: `admin`

Stop services:

```bash
cd airflow
docker compose down
```

### Scale for Larger Files

```bash
# For 10 GB+ files, increase workers
terraform apply \
  -var="bucket_name=my-search-keyword-data" \
  -var="glue_worker_count=10" \
  -var="glue_worker_type=G.2X"
```

### Tear Down

```bash
terraform destroy -var="bucket_name=my-search-keyword-data"
```

## Output Format

Local CLI output is a tab-delimited `.tab` file sorted by Revenue (descending):

| Search Engine Domain | Search Keyword | Revenue |
|---|---|---|
| google.com | ipod | 480.00 |
| bing.com | zune | 250.00 |

Glue (10GB+ path) output is written as Parquet files under:
`output/YYYY-mm-dd_SearchKeywordPerformance/part-*.parquet`

Lambda (small-file path) output is written as:
`output/<input-stem>_YYYY-mm-dd_SearchKeywordPerformance.parquet`

## Project Structure

```
search-keyword-performance/
├── README.md
├── requirements.txt
├── airflow/
│   └── dags/
│       └── search_keyword_pipeline_dag.py # Glue orchestration DAG
├── terraform/
│   ├── main.tf                    # Glue + Lambda + S3 + IAM resources
│   ├── variables.tf               # Configurable inputs (workers, bucket)
│   └── outputs.tf                 # Exported job name and bucket ARN
├── src/
│   ├── __init__.py
│   ├── main.py                    # CLI entry point (local)
│   ├── search_keyword_analyzer.py # Core analysis class (streaming)
│   ├── glue_job.py                # AWS Glue ETL job (Spark, scales to 10GB+)
│   └── lambda_handler.py          # Lightweight Lambda handler (small files)
└── tests/
    ├── __init__.py
    └── test_search_keyword_analyzer.py
```

## Scalability Considerations (10 GB+ files)

### Current Design

The local CLI uses `csv.DictReader` to stream line-by-line -- memory usage is proportional to unique visitors/keywords, not file size. This works well for single-machine processing.

### Production Scale with Glue

The `glue_job.py` uses PySpark to distribute processing across a cluster:

| Concern | How Glue Handles It |
|---|---|
| **File size** | Spark partitions the input across workers automatically. A 10 GB TSV is split into ~128 MB chunks read in parallel. |
| **Compute** | Scale horizontally by increasing `glue_worker_count`. G.2X workers provide 8 vCPU / 32 GB each. |
| **Sessionization** | Spark window functions (`row_number` over `visitor_id` partitioned by `hit_time_gmt`) replace the in-memory dict for last-touch attribution. |
| **Cost** | Glue charges per DPU-hour. A 10 GB file with 10 G.1X workers finishes in minutes (~$0.50). |
| **Further optimization** | Convert TSV to Parquet with a Glue Crawler for columnar reads. Add a Glue Trigger on S3 events for fully automated ingestion. |

## Future Considerations (Scaling Roadmap)

This project is designed to be deployable and correct today. In a production setting (especially with 10GB+ uncompressed input and ongoing BI/insights), the next steps would focus on optimizing storage format and adding enterprise-ready “last-mile” query layers.

### Phase 1 (Current Implementation)
Base Layer that works and is deployable:
1. **Small files**: S3-triggered **AWS Lambda** writes daily `.tab` results to `s3://<bucket>/output/`.
2. **Large files (10GB+)**: **Airflow + AWS Glue (Spark)** processes hit-level TSV at scale and writes aggregated results back to S3.
3. **Orchestration**: Airflow coordinates Glue execution and performs a run-date scoped S3 output check.

### Phase 2 (Implemented): Optimize the Data Lake with Parquet
**Current**: Glue writes **Apache Parquet** into a date-scoped S3 folder:
`output/YYYY-mm-dd_SearchKeywordPerformance/part-*.parquet`

**Why**:
- Parquet reduces storage cost and improves read performance.
- It enables faster and cheaper analytics queries because query engines can read only the required columns.
- Parquet works especially well with **Athena** and **Redshift Spectrum**-style patterns.

**Operational notes**:
- Partition by date (for example: `year=YYYY/month=MM/day=DD`) to keep scans efficient.
- Consider writing curated tables for BI consumption (CTAS-style) rather than querying raw job output.

### Phase 3 (BI Layer): Amazon Redshift Serverless
**Recommendation**: Load the Parquet-based aggregates into **Amazon Redshift Serverless** for BI workloads.

**How it would work**:
- Airflow runs the Glue job (Phase 1/2).
- Airflow then bulk-loads the curated Parquet outputs into Redshift using an S3-to-Redshift loading operator/pattern.
- BI tools (Tableau/Power BI) connect to Redshift for fast SQL queries over the aggregated results.

### Phase 4 (AI/Agentic “Wow” Factor): Aurora PostgreSQL + pgvector
**Recommendation**: Export the aggregated keyword performance data (and/or engineered text/embeddings) into **Aurora PostgreSQL**.

**Why**:
- Aurora PostgreSQL supports strong SQL for analytics.
- With **pgvector**, you can store embeddings for keywords (or phrases) and enable semantic retrieval.
- This allows an AI agent to answer “why” questions using both structured aggregates and semantic similarity, for example:
  - “Why did Google’s revenue drop while Yahoo increased this week?”

### How to present this in your review
Use a phased story:
1. **Phase 1 (Current)**: Lambda (small files) + Airflow/Glue (10GB+ files) writing aggregates to an S3 data lake.
2. **Phase 2 (Implemented)**: Write Parquet lake outputs (date-scoped) for faster BI reads.
3. **Phase 3 (Enterprise)**: Add Redshift for BI dashboards.
4. **Phase 4 (Enterprise AI)**: Add Aurora + pgvector to power agentic explanations and semantic retrieval.
