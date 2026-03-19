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

**Lambda mode**: S3 upload to `input/` triggers Lambda for lightweight processing and writes a `.tab` file to `output/`.

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

The Lambda path writes a tab file directly to:
`s3://my-search-keyword-data/output/YYYY-mm-dd_SearchKeywordPerformance.tab`

## Airflow Orchestration

An example Airflow DAG is provided at:
`airflow/dags/search_keyword_pipeline_dag.py`

It performs:
1. Trigger Glue job
2. Wait for Glue completion
3. Verify output exists in S3

### Airflow dependencies

Install Amazon provider in your Airflow environment:

```bash
pip install apache-airflow-providers-amazon
```

Configure Airflow connection `aws_default` with AWS credentials/region, then trigger DAG `search_keyword_glue_pipeline`.

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

Tab-delimited file sorted by Revenue (descending):

| Search Engine Domain | Search Keyword | Revenue |
|---|---|---|
| google.com | ipod | 480.00 |
| bing.com | zune | 250.00 |

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
