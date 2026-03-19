"""
Airflow DAG to orchestrate the search keyword pipeline via AWS Glue.

Flow:
1) Start Glue job
2) Wait for Glue job completion
3) Verify output objects exist under s3://<bucket>/output/
"""

from __future__ import annotations

from datetime import datetime, timedelta

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

    start_glue >> wait_for_glue >> verify_output
