"""
AWS Lambda handler -- triggered by S3 PutObject events on the **landing** layer.

Streams the hit-level data file directly from S3, runs the analyzer
in-memory (rows are sorted by hit_time_gmt), and uploads aggregated results
to the **curated** prefix (see ``docs/medallion_architecture.md``).
"""

import codecs
import io
import logging
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote_plus

from botocore.exceptions import ClientError
import boto3

try:
    # When running locally from the repo root.
    from src.partition_time import partition_from_unix_seconds, partition_prefix
    from src.search_keyword_analyzer import SearchKeywordAnalyzer
except ImportError:  # pragma: no cover
    # When running from the Lambda zip (Terraform zips `../src`).
    from partition_time import partition_from_unix_seconds, partition_prefix
    from search_keyword_analyzer import SearchKeywordAnalyzer

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
ssm = boto3.client("ssm")
secrets = boto3.client("secretsmanager")
# Curated (gold) layer — align with Terraform ``output_prefix`` (default ``curated/search_keyword/``).
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "curated/search_keyword/")
# PARTITION_INTERVAL_MINUTES: optional; defaults in partition_time.py. In AWS, set via Terraform
# ``partition_interval_minutes`` so Lambda matches Glue ``--partition_interval_minutes``.
# When true (default), write under dt=/hour=/minute=00|15|30|45 from earliest hit_time_gmt (UTC).
PARTITION_OUTPUT_KEYS = os.environ.get("PARTITION_OUTPUT_KEYS", "true").lower() == "true"
API_KEY_PARAM = os.environ.get("API_KEY_PARAM", "")
SYNC_DB_SINKS = os.environ.get("SYNC_DB_SINKS", "false").lower() == "true"
DB_HOST = os.environ.get("DB_HOST", "")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "")
DB_SECRET_ARN = os.environ.get("DB_SECRET_ARN", "")
DB_FACT_TABLE = os.environ.get("DB_FACT_TABLE", "fact_keyword_performance")
DB_AI_TABLE = os.environ.get("DB_AI_TABLE", "ai_keyword_insights")

_PG_IDENT = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_pg_identifier(name: str, label: str) -> str:
    if not name or not _PG_IDENT.fullmatch(name):
        raise ValueError(f"Invalid {label} identifier {name!r} (use letters, numbers, underscore; no spaces).")
    return name


def _run_date_utc() -> str:
    """Calendar date in UTC (aligns with Glue DB sink and Airflow verification)."""
    return datetime.now(timezone.utc).date().isoformat()


def _partition_subprefix(analyzer: SearchKeywordAnalyzer) -> str:
    """Hive-style path segment, or empty when flat output is requested."""
    if not PARTITION_OUTPUT_KEYS:
        return ""
    tp = analyzer.earliest_hit_partition_utc()
    if tp is None:
        tp = partition_from_unix_seconds(int(datetime.now(timezone.utc).timestamp()))
    return partition_prefix(tp) + "/"


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


def _is_parquet_blob(blob: bytes) -> bool:
    """Parquet files start with magic bytes PAR1."""
    return len(blob) >= 4 and blob[:4] == b"PAR1"


def _write_parquet(records: list, output_dir: str, input_stem: str) -> Path:
    """Write analyzer output to a Parquet file and return its path."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as e:
        raise RuntimeError(
            "Parquet output requires pyarrow in the Lambda runtime (bundle pyarrow in the deployment zip)."
        ) from e

    output_path = Path(output_dir) / (
        f"{input_stem}_{_run_date_utc()}_SearchKeywordPerformance.parquet"
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


def _load_db_credentials() -> tuple[str, str]:
    secret_value = secrets.get_secret_value(SecretId=DB_SECRET_ARN)
    secret_string = secret_value.get("SecretString", "")
    if not secret_string:
        raise RuntimeError("DB secret does not contain SecretString.")
    import json

    payload = json.loads(secret_string)
    user = payload.get("username")
    password = payload.get("password")
    if not user or not password:
        raise RuntimeError("DB secret missing username/password keys.")
    return user, password


def _dedupe_ai_insights_table(cur, ai_table: str) -> None:
    """
    Keep one row per (search_engine_domain, search_keyword) — highest keyword_id wins.
    Required before CREATE UNIQUE INDEX if the table already had duplicate rows (e.g. legacy inserts).
    """
    cur.execute(
        f"""
        DELETE FROM {ai_table} a
        USING {ai_table} b
        WHERE a.keyword_id < b.keyword_id
          AND a.search_engine_domain = b.search_engine_domain
          AND a.search_keyword = b.search_keyword
        """
    )


def _sync_to_postgres(records: list, run_date: str) -> None:
    if not (DB_HOST and DB_NAME and DB_SECRET_ARN):
        logger.warning("Postgres sink config missing; skipping DB sync.")
        return

    fact_table = _validate_pg_identifier(DB_FACT_TABLE, "DB_FACT_TABLE")
    ai_table = _validate_pg_identifier(DB_AI_TABLE, "DB_AI_TABLE")
    ai_uk = _validate_pg_identifier(f"uq_{ai_table}_engine_kw", "AI unique index name")

    user, password = _load_db_credentials()
    import pg8000

    conn = pg8000.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=user,
        password=password,
        database=DB_NAME,
        timeout=20,
    )
    cur = conn.cursor()
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {fact_table} (
            event_date DATE,
            search_engine_domain TEXT,
            search_keyword TEXT,
            total_revenue NUMERIC(18,2)
        )
        """
    )
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {ai_table} (
            keyword_id SERIAL PRIMARY KEY,
            search_engine_domain TEXT,
            search_keyword TEXT,
            revenue_impact_score DOUBLE PRECISION,
            last_processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    _dedupe_ai_insights_table(cur, ai_table)
    cur.execute(
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS {ai_uk}
        ON {ai_table} (search_engine_domain, search_keyword)
        """
    )
    cur.execute(f"DELETE FROM {fact_table} WHERE event_date = %s", (run_date,))
    for rec in records:
        cur.execute(
            f"""
            INSERT INTO {fact_table}
            (event_date, search_engine_domain, search_keyword, total_revenue)
            VALUES (%s, %s, %s, %s)
            """,
            (run_date, rec.engine_domain, rec.keyword, float(rec.revenue)),
        )
        cur.execute(
            f"""
            INSERT INTO {ai_table}
            (search_engine_domain, search_keyword, revenue_impact_score)
            VALUES (%s, %s, %s)
            ON CONFLICT (search_engine_domain, search_keyword)
            DO UPDATE SET
              revenue_impact_score = EXCLUDED.revenue_impact_score,
              last_processed_at = CURRENT_TIMESTAMP
            """,
            (rec.engine_domain, rec.keyword, float(rec.revenue)),
        )
    conn.commit()
    cur.close()
    conn.close()


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
        raw = response["Body"].read()
        key_lower = key.lower()

        analyzer = SearchKeywordAnalyzer()
        if key_lower.endswith(".parquet") or _is_parquet_blob(raw):
            records = analyzer.process_parquet_bytes(raw)
        else:
            stream = codecs.getreader("utf-8")(io.BytesIO(raw))
            records = analyzer.process_stream(stream)

        with tempfile.TemporaryDirectory() as tmpdir:
            input_stem = Path(key).stem or key.replace("/", "_")
            if SYNC_DB_SINKS:
                run_date = _run_date_utc()
                _sync_to_postgres(records, run_date)
                logger.info("Synced Lambda output to configured DB sinks.")

            output_path = _write_parquet(records, tmpdir, input_stem)
            sub = _partition_subprefix(analyzer)
            output_key = f"{OUTPUT_PREFIX}{sub}{output_path.name}"
            s3.upload_file(str(output_path), bucket, output_key)
            logger.info("Uploaded results to s3://%s/%s", bucket, output_key)

    return {"statusCode": 200, "body": "OK"}
