"""
AWS Lambda handler -- triggered by S3 PutObject events.

Streams the hit-level data file directly from S3, runs the analyzer
in-memory (rows are sorted by hit_time_gmt), and uploads the result
back to the same bucket.
"""

import codecs
import logging
import os
import tempfile
from datetime import date
from pathlib import Path
from urllib.parse import unquote_plus

from botocore.exceptions import ClientError
import boto3

try:
    # When running locally from the repo root.
    from src.search_keyword_analyzer import SearchKeywordAnalyzer
except ImportError:  # pragma: no cover
    # When running from the Lambda zip (Terraform zips `../src`).
    from search_keyword_analyzer import SearchKeywordAnalyzer

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
ssm = boto3.client("ssm")
redshift_data = boto3.client("redshift-data")
rds_data = boto3.client("rds-data")
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "output/")
API_KEY_PARAM = os.environ.get("API_KEY_PARAM", "")
SYNC_DB_SINKS = os.environ.get("SYNC_DB_SINKS", "false").lower() == "true"
REDSHIFT_WORKGROUP_NAME = os.environ.get("REDSHIFT_WORKGROUP_NAME", "")
REDSHIFT_DATABASE = os.environ.get("REDSHIFT_DATABASE", "")
REDSHIFT_SECRET_ARN = os.environ.get("REDSHIFT_SECRET_ARN", "")
REDSHIFT_FACT_TABLE = os.environ.get("REDSHIFT_FACT_TABLE", "fact_keyword_performance")
AURORA_CLUSTER_ARN = os.environ.get("AURORA_CLUSTER_ARN", "")
AURORA_DATABASE = os.environ.get("AURORA_DATABASE", "")
AURORA_SECRET_ARN = os.environ.get("AURORA_SECRET_ARN", "")
AURORA_AI_TABLE = os.environ.get("AURORA_AI_TABLE", "ai_keyword_insights")


def _load_api_key_from_ssm() -> str:
    """Load an optional API key from SSM Parameter Store."""
    if not API_KEY_PARAM:
        return ""
    try:
        response = ssm.get_parameter(Name=API_KEY_PARAM, WithDecryption=True)
        return response["Parameter"]["Value"]
    except ClientError as e:
        # Don't log secret values; just record error class/code.
        err_code = (e.response.get("Error") or {}).get("Code", "ClientError")
        logger.warning(
            "Failed to load SSM parameter %s (%s). Continuing without API key.",
            API_KEY_PARAM,
            err_code,
        )
        return ""


def _write_parquet(records: list, output_dir: str, input_stem: str) -> Path:
    """Write analyzer output to a Parquet file and return its path."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as e:
        raise RuntimeError(
            "Parquet output requires pyarrow in the Lambda runtime. "
            "Attach a Lambda layer or package pyarrow with the function."
        ) from e

    output_path = Path(output_dir) / (
        f"{input_stem}_{date.today().isoformat()}_SearchKeywordPerformance.parquet"
    )
    table = pa.Table.from_pydict(
        {
            "Search Engine Domain": [r.engine_domain for r in records],
            "Search Keyword": [r.keyword for r in records],
            "Revenue": [round(float(r.revenue), 2) for r in records],
        }
    )
    pq.write_table(table, output_path)
    return output_path


def _sync_to_redshift(records: list, run_date: str) -> None:
    if not (REDSHIFT_WORKGROUP_NAME and REDSHIFT_DATABASE and REDSHIFT_SECRET_ARN):
        logger.warning("Redshift sink config missing; skipping Redshift sync.")
        return

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {REDSHIFT_FACT_TABLE} (
        event_date DATE,
        search_engine_domain VARCHAR(100),
        search_keyword VARCHAR(500),
        total_revenue DECIMAL(18,2)
    );
    """
    redshift_data.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP_NAME,
        Database=REDSHIFT_DATABASE,
        SecretArn=REDSHIFT_SECRET_ARN,
        Sql=create_sql,
    )
    redshift_data.execute_statement(
        WorkgroupName=REDSHIFT_WORKGROUP_NAME,
        Database=REDSHIFT_DATABASE,
        SecretArn=REDSHIFT_SECRET_ARN,
        Sql=f"DELETE FROM {REDSHIFT_FACT_TABLE} WHERE event_date = :d",
        Parameters=[{"name": "d", "value": {"stringValue": run_date}}],
    )
    for rec in records:
        redshift_data.execute_statement(
            WorkgroupName=REDSHIFT_WORKGROUP_NAME,
            Database=REDSHIFT_DATABASE,
            SecretArn=REDSHIFT_SECRET_ARN,
            Sql=f"""
            INSERT INTO {REDSHIFT_FACT_TABLE}
            (event_date, search_engine_domain, search_keyword, total_revenue)
            VALUES (:d, :engine, :keyword, :revenue)
            """,
            Parameters=[
                {"name": "d", "value": {"stringValue": run_date}},
                {"name": "engine", "value": {"stringValue": rec.engine_domain}},
                {"name": "keyword", "value": {"stringValue": rec.keyword}},
                {"name": "revenue", "value": {"doubleValue": float(rec.revenue)}},
            ],
        )


def _sync_to_aurora(records: list) -> None:
    if not (AURORA_CLUSTER_ARN and AURORA_DATABASE and AURORA_SECRET_ARN):
        logger.warning("Aurora sink config missing; skipping Aurora sync.")
        return

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {AURORA_AI_TABLE} (
        keyword_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        search_engine_domain TEXT,
        search_keyword TEXT,
        revenue_impact_score DOUBLE PRECISION,
        last_processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    rds_data.execute_statement(
        resourceArn=AURORA_CLUSTER_ARN,
        secretArn=AURORA_SECRET_ARN,
        database=AURORA_DATABASE,
        sql=create_sql,
    )
    for rec in records:
        rds_data.execute_statement(
            resourceArn=AURORA_CLUSTER_ARN,
            secretArn=AURORA_SECRET_ARN,
            database=AURORA_DATABASE,
            sql=f"""
            INSERT INTO {AURORA_AI_TABLE}
            (search_engine_domain, search_keyword, revenue_impact_score)
            VALUES (:engine, :keyword, :revenue)
            """,
            parameters=[
                {"name": "engine", "value": {"stringValue": rec.engine_domain}},
                {"name": "keyword", "value": {"stringValue": rec.keyword}},
                {"name": "revenue", "value": {"doubleValue": float(rec.revenue)}},
            ],
        )


def handler(event: dict, context: object) -> dict:
    if API_KEY_PARAM:
        # Optional pattern: validate secret retrieval without logging secret value.
        api_key = _load_api_key_from_ssm()
        if api_key:
            logger.info("Loaded API key from SSM parameter %s", API_KEY_PARAM)
        else:
            logger.warning("API key not available from SSM parameter %s", API_KEY_PARAM)

    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])
        logger.info("Processing s3://%s/%s", bucket, key)

        response = s3.get_object(Bucket=bucket, Key=key)
        stream = codecs.getreader("utf-8")(response["Body"])

        analyzer = SearchKeywordAnalyzer()
        records = analyzer.process_stream(stream)

        with tempfile.TemporaryDirectory() as tmpdir:
            input_stem = Path(key).stem or key.replace("/", "_")
            output_path = _write_parquet(records, tmpdir, input_stem)
            output_key = f"{OUTPUT_PREFIX}{output_path.name}"
            s3.upload_file(str(output_path), bucket, output_key)
            logger.info("Uploaded results to s3://%s/%s", bucket, output_key)

            if SYNC_DB_SINKS:
                run_date = date.today().isoformat()
                _sync_to_redshift(records, run_date)
                _sync_to_aurora(records)
                logger.info("Synced Lambda output to configured DB sinks.")

    return {"statusCode": 200, "body": "OK"}
