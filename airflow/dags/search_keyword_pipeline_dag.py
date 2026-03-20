"""
Airflow DAG to orchestrate the search keyword pipeline via AWS Glue.

Flow:
1) Start Glue job
2) Wait for Glue job completion
3) Verify output objects exist under s3://<bucket>/output/
4) If sync_db_sinks is true: verify Postgres fact + AI tables received data for this run
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor

AWS_CONN_ID = "aws_default"
GLUE_JOB_NAME = "search-keyword-performance"
S3_BUCKET = Variable.get("search_keyword_bucket", default_var="acs-keyword-revenue-nayanaj")
S3_OUTPUT_PREFIX = "output/"
SYNC_DB_SINKS = Variable.get("sync_db_sinks", default_var="false")
DB_HOST = Variable.get("db_host", default_var="")
DB_PORT = Variable.get("db_port", default_var="5432")
DB_NAME = Variable.get("db_name", default_var="")
DB_SECRET_ARN = Variable.get("db_secret_arn", default_var="")
DB_FACT_TABLE = Variable.get("db_fact_table", default_var="fact_keyword_performance")
DB_AI_TABLE = Variable.get("db_ai_table", default_var="ai_keyword_insights")


def verify_output_exists(bucket_name: str, prefix: str, aws_conn_id: str, **context) -> None:
    """Fail task if no output objects for this run's date are found under the prefix."""
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    run_date = context.get("ds")  # e.g. "2026-03-18"
    date_scoped_prefix = f"{prefix}{run_date}_SearchKeywordPerformance"
    keys = s3.list_keys(bucket_name=bucket_name, prefix=date_scoped_prefix)
    if not keys:
        raise AirflowException(
            f"No output objects found in s3://{bucket_name}/{date_scoped_prefix}"
        )


def _validate_pg_identifier(name: str, label: str) -> str:
    if not name or not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", name):
        raise AirflowException(f"Invalid {label} identifier: {name!r} (expected simple SQL name).")
    return name


def verify_db_sinks_e2e(**context) -> None:
    """
    When Variable sync_db_sinks is true, confirm Glue's DB write path worked:
    fact rows for UTC 'today' (same convention as Glue/Lambda) and recent AI rows.
    Skips quietly when sync_db_sinks is false so DAGs without DB stay unchanged.
    """
    sync_raw = Variable.get("sync_db_sinks", default_var="false")
    if str(sync_raw).lower() not in ("true", "1", "yes"):
        logging.info("sync_db_sinks disabled; skipping DB end-to-end check.")
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
            "sync_db_sinks=true but db_host, db_name, or db_secret_arn Airflow Variable is empty."
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

    # Glue/Lambda use date.today().isoformat() for event_date (scheduler/worker local date).
    run_date = date.today().isoformat()
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
            f"DB E2E check failed: no rows in {db_fact} for event_date={run_date!r} "
            f"(Glue uses today's date in the worker timezone; align Airflow worker TZ or DAG schedule if needed)."
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
            "--sync_db_sinks": SYNC_DB_SINKS,
            "--db_host": DB_HOST,
            "--db_port": DB_PORT,
            "--db_name": DB_NAME,
            "--db_secret_arn": DB_SECRET_ARN,
            "--db_fact_table": DB_FACT_TABLE,
            "--db_ai_table": DB_AI_TABLE,
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
            "bucket_name": S3_BUCKET,
            "prefix": S3_OUTPUT_PREFIX,
            "aws_conn_id": AWS_CONN_ID,
        },
    )

    verify_db = PythonOperator(
        task_id="verify_db_sinks_e2e",
        python_callable=verify_db_sinks_e2e,
    )

    start_glue >> wait_for_glue >> verify_output >> verify_db
