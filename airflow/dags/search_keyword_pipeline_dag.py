"""
Airflow DAG — full AWS pipeline: **Glue** → **S3 verify** → **Lambda** (optional) → **DB verify** (optional).

**Data bucket (first match wins):** Airflow Variable ``search_keyword_bucket``, then env
``SEARCH_KEYWORD_DATA_BUCKET``, then optional SSM parameter ``DATA_BUCKET_SSM_PARAM`` (if you create it).

Requires **Apache Airflow 2.x** (Jinja ``var.value.get(key, default)`` for optional Variables).

Flow:
1) Start Glue job
2) Wait for Glue job completion
3) Verify Glue output objects under curated prefix
4) If ``glue_sync_db_sinks`` is true: verify Postgres (Glue DB writes)
5) Optionally upload a tiny **Parquet** file to ``landing/dt=<ds>/`` and **invoke** Lambda (same contract as S3 trigger)

Variables:
- ``glue_sync_db_sinks`` — default **off** (``false``). Set ``true`` only when Terraform grants Glue
  ``secretsmanager:GetSecretValue`` on ``db_secret_arn`` and RDS is configured — otherwise Glue will fail on Secrets Manager.
- ``sync_db_sinks`` — deprecated; use ``glue_sync_db_sinks`` for both Glue and DB verify.
- ``airflow_invoke_lambda`` — default ``true``; set ``false`` to skip the Lambda smoke task.
- ``lambda_function_name`` — default ``search-keyword-performance``.
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor

AWS_CONN_ID = "aws_default"
GLUE_JOB_NAME = "search-keyword-performance"
# Curated (gold) layer — align with Terraform ``output_prefix`` (default ``curated/search_keyword/``).
S3_OUTPUT_PREFIX = "curated/search_keyword/"

# Optional SSM parameter name if you store the data bucket name in Parameter Store.
DATA_BUCKET_SSM_PARAM = "/search-keyword-performance/airflow/data_bucket_name"

# Packaged next to this DAG — one-row Parquet (same schema as sample_hit_data; PyArrow not required in workers).
_LAMBDA_SMOKE_PARQUET = Path(__file__).resolve().parent / "lambda_smoke_sample.parquet"


def _resolve_data_bucket() -> str:
    """
    Resolve the S3 *data* bucket (landing/curated).

    Order: Variable ``search_keyword_bucket`` → env ``SEARCH_KEYWORD_DATA_BUCKET`` → SSM parameter.
    """
    v = Variable.get("search_keyword_bucket", default_var="").strip()
    if v:
        return v
    env_bucket = os.environ.get("SEARCH_KEYWORD_DATA_BUCKET", "").strip()
    if env_bucket:
        return env_bucket
    param = Variable.get("data_bucket_ssm_parameter", default_var="").strip() or DATA_BUCKET_SSM_PARAM
    try:
        import boto3

        ssm = boto3.client("ssm")
        r = ssm.get_parameter(Name=param, WithDecryption=False)
        return (r.get("Parameter") or {}).get("Value", "").strip()
    except Exception as exc:
        logging.debug("SSM lookup %s failed: %s", param, exc)
        return ""


def verify_output_exists(prefix: str, aws_conn_id: str, **context) -> None:
    """Fail task if no output objects for this run's date are found under the prefix."""
    bucket_name = _resolve_data_bucket()
    if not bucket_name:
        raise AirflowException(
            "Could not resolve the S3 data bucket. Set Airflow Variable 'search_keyword_bucket', "
            "or environment variable SEARCH_KEYWORD_DATA_BUCKET on workers, "
            f"or create SSM parameter {DATA_BUCKET_SSM_PARAM!r} with the bucket name."
        )
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    run_date = context.get("ds")  # e.g. "2026-03-18"
    # Supports flat keys (output/YYYY-MM-DD_...) and partitioned keys
    # (output/dt=.../hour=.../minute=00|15|30|45/..._YYYY-MM-DD_...).
    needle = f"{run_date}_SearchKeywordPerformance"
    keys = s3.list_keys(bucket_name=bucket_name, prefix=prefix) or []
    matched = [k for k in keys if needle in k and not k.endswith("/")]
    if not matched:
        raise AirflowException(
            f"No output objects found for {needle!r} under s3://{bucket_name}/{prefix}"
        )


def _validate_pg_identifier(name: str, label: str) -> str:
    if not name or not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", name):
        raise AirflowException(f"Invalid {label} identifier: {name!r} (expected simple SQL name).")
    return name


def verify_db_sinks_e2e(**context) -> None:
    """
    When Variable glue_sync_db_sinks is true, confirm Glue's DB write path worked.
    Skips quietly when false so S3/Glue-only runs stay unchanged.
    """
    sync_raw = Variable.get("glue_sync_db_sinks", default_var="false")
    if str(sync_raw).lower() not in ("true", "1", "yes"):
        logging.info("glue_sync_db_sinks disabled; skipping DB end-to-end check.")
        return
    db_verify_mode = Variable.get("db_verify_mode", default_var="auto").strip().lower()
    if db_verify_mode not in {"auto", "strict"}:
        raise AirflowException("db_verify_mode must be either 'auto' or 'strict'.")

    db_host = Variable.get("db_host", default_var="").strip()
    db_port = int(Variable.get("db_port", default_var="5432") or "5432")
    db_name = Variable.get("db_name", default_var="").strip()
    db_secret_arn = Variable.get("db_secret_arn", default_var="").strip()
    db_fact = _validate_pg_identifier(
        Variable.get("db_fact_table", default_var="fact_keyword_performance"), "db_fact_table"
    )
    db_ai = _validate_pg_identifier(
        Variable.get("db_ai_table", default_var="ai_keyword_insights"), "db_ai_table"
    )

    if not all([db_host, db_name, db_secret_arn]):
        raise AirflowException(
            "glue_sync_db_sinks=true but db_host, db_name, or db_secret_arn Airflow Variable is empty."
        )

    import boto3
    import pg8000

    secrets = boto3.client("secretsmanager")
    secret_value = secrets.get_secret_value(SecretId=db_secret_arn)
    payload = json.loads(secret_value.get("SecretString") or "{}")
    user = payload.get("username")
    password = payload.get("password")
    if not user or not password:
        raise AirflowException("DB secret missing username/password keys.")

    run_date = datetime.now(timezone.utc).date().isoformat()
    try:
        conn = pg8000.connect(
            host=db_host,
            port=db_port,
            user=user,
            password=password,
            database=db_name,
            timeout=60,
        )
        try:
            cur = conn.cursor()
            cur.execute(
                f"SELECT COUNT(*) FROM {db_fact} WHERE event_date = CAST(%s AS DATE)",
                (run_date,),
            )
            fact_n = int(cur.fetchone()[0])
            cur.execute(
                f"""
                SELECT COUNT(*) FROM {db_ai}
                WHERE last_processed_at >= NOW() - INTERVAL '2 hours'
                """
            )
            ai_recent = int(cur.fetchone()[0])
            cur.close()
        finally:
            conn.close()
    except Exception as exc:
        if db_verify_mode == "auto":
            logging.warning(
                "DB verification skipped in auto mode: cannot reach %s:%s (%s). "
                "Set Airflow Variable db_verify_mode=strict to fail on connectivity issues.",
                db_host,
                db_port,
                exc,
            )
            return
        raise AirflowException(
            f"DB verification failed in strict mode: cannot reach {db_host}:{db_port}: {exc}"
        ) from exc

    if fact_n < 1:
        raise AirflowException(
            f"DB E2E check failed: no rows in {db_fact} for event_date={run_date!r} (UTC)."
        )
    if ai_recent < 1:
        raise AirflowException(
            f"DB E2E check failed: no rows in {db_ai} with last_processed_at within the last 2 hours."
        )

    logging.info(
        "DB E2E OK: %s rows for %s in %s; %s recent rows in %s.",
        fact_n,
        run_date,
        db_fact,
        ai_recent,
        db_ai,
    )


def invoke_lambda_pipeline(**context) -> None:
    """
    Optional: put a tiny Parquet file under landing/dt=<ds>/ and invoke the Lambda synchronously
    (same contract as S3 PutObject → Lambda). Skipped when Variable airflow_invoke_lambda is false.
    """
    opt = Variable.get("airflow_invoke_lambda", default_var="true")
    if str(opt).lower() not in ("true", "1", "yes"):
        logging.info("airflow_invoke_lambda disabled; skipping Lambda invoke.")
        return

    bucket = _resolve_data_bucket()
    if not bucket:
        raise AirflowException(
            f"Could not resolve S3 bucket for Lambda smoke (Variable search_keyword_bucket or SSM {DATA_BUCKET_SSM_PARAM})."
        )

    fn = Variable.get("lambda_function_name", default_var="search-keyword-performance").strip()
    ds = context.get("ds") or ""
    run_id = str(context.get("run_id") or "manual")
    safe_rid = re.sub(r"[^a-zA-Z0-9._-]", "_", run_id)[:120]
    key = f"landing/dt={ds}/airflow_lambda_{safe_rid}.parquet"

    import boto3

    if not _LAMBDA_SMOKE_PARQUET.is_file():
        raise AirflowException(
            f"Missing packaged Parquet for Lambda smoke: {_LAMBDA_SMOKE_PARQUET} "
            "(sync airflow/dags/ from the repo, including lambda_smoke_sample.parquet)."
        )
    body = _LAMBDA_SMOKE_PARQUET.read_bytes()

    region = Variable.get("aws_default_region", default_var="").strip() or None
    s3_kw = {"region_name": region} if region else {}
    s3c = boto3.client("s3", **s3_kw)
    s3c.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/vnd.apache.parquet",
    )
    logging.info("Uploaded Lambda smoke input to s3://%s/%s", bucket, key)

    payload = {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "awsRegion": region or "",
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {"name": bucket},
                    "object": {"key": key},
                },
            }
        ]
    }
    lam_kw = {"region_name": region} if region else {}
    lam = boto3.client("lambda", **lam_kw)
    resp = lam.invoke(
        FunctionName=fn,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode("utf-8"),
    )
    raw = resp["Payload"].read().decode("utf-8", errors="replace")
    if resp.get("FunctionError"):
        raise AirflowException(f"Lambda {fn} failed: {raw}")
    logging.info("Lambda %s OK: %s", fn, raw[:500])


with DAG(
    dag_id="search_keyword_glue_pipeline",
    description="Trigger and monitor Glue job for search keyword analysis.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["aws", "glue", "search-keyword"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    start_glue = GlueJobOperator(
        task_id="start_glue_job",
        job_name=GLUE_JOB_NAME,
        aws_conn_id=AWS_CONN_ID,
        wait_for_completion=False,
        script_args={
            # Partition pruning: defaults use ds / empty so the DAG renders if Variables are missing.
            # Optional overrides: glue_partition_dt, glue_partition_hour, glue_partition_minute.
            "--partition_dt": "{{ var.value.get('glue_partition_dt', ds) }}",
            "--partition_hour": "{{ var.value.get('glue_partition_hour', '') }}",
            "--partition_minute": "{{ var.value.get('glue_partition_minute', '') }}",
            "--partition_interval_minutes": "{{ var.value.get('glue_partition_interval_minutes', '15') }}",
            # Off by default so Glue does not call Secrets Manager until Terraform IAM + db_secret_arn match.
            "--sync_db_sinks": "{{ var.value.get('glue_sync_db_sinks', 'false') }}",
            "--db_host": "{{ var.value.get('db_host', '') }}",
            "--db_port": "{{ var.value.get('db_port', '5432') }}",
            "--db_name": "{{ var.value.get('db_name', '') }}",
            "--db_secret_arn": "{{ var.value.get('db_secret_arn', '') }}",
            "--db_fact_table": "{{ var.value.get('db_fact_table', 'fact_keyword_performance') }}",
            "--db_ai_table": "{{ var.value.get('db_ai_table', 'ai_keyword_insights') }}",
            # Optional Spark tuning (defaults match Terraform; override for heavy runs):
            "--enable_large_job_optimizations": "{{ var.value.get('glue_enable_large_job_optimizations', 'false') }}",
            "--shuffle_partitions": "{{ var.value.get('glue_shuffle_partitions', '200') }}",
            "--curated_output_partitions": "{{ var.value.get('glue_curated_output_partitions', '1') }}",
            "--visitor_repartition_partitions": "{{ var.value.get('glue_visitor_repartition_partitions', '0') }}",
            "--staging_repartition_partitions": "{{ var.value.get('glue_staging_repartition_partitions', '0') }}",
            "--s3_recursive_list": "{{ var.value.get('glue_s3_recursive_list', 'true') }}",
        },
    )

    wait_for_glue = GlueJobSensor(
        task_id="wait_for_glue_job",
        job_name=GLUE_JOB_NAME,
        run_id=start_glue.output,
        aws_conn_id=AWS_CONN_ID,
        poke_interval=30,
        timeout=60 * 60,
    )

    verify_output = PythonOperator(
        task_id="verify_s3_output",
        python_callable=verify_output_exists,
        op_kwargs={
            "prefix": S3_OUTPUT_PREFIX,
            "aws_conn_id": AWS_CONN_ID,
        },
    )

    invoke_lambda = PythonOperator(
        task_id="invoke_lambda_smoke",
        python_callable=invoke_lambda_pipeline,
    )

    verify_db = PythonOperator(
        task_id="verify_db_sinks_e2e",
        python_callable=verify_db_sinks_e2e,
    )

    start_glue >> wait_for_glue >> verify_output >> verify_db >> invoke_lambda
