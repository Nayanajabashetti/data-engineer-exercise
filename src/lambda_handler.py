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
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "output/")
API_KEY_PARAM = os.environ.get("API_KEY_PARAM", "")


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
            output_path = analyzer.write_output(records, tmpdir)
            input_stem = Path(key).stem or key.replace("/", "_")
            output_key = f"{OUTPUT_PREFIX}{input_stem}_{output_path.name}"
            s3.upload_file(str(output_path), bucket, output_key)
            logger.info("Uploaded results to s3://%s/%s", bucket, output_key)

    return {"statusCode": 200, "body": "OK"}
