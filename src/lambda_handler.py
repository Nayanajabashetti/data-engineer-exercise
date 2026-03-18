"""
AWS Lambda handler -- triggered by S3 PutObject events.

Streams the hit-level data file directly from S3 (no disk download),
runs the analyzer, and uploads the result back to the same bucket.
"""

import codecs
import logging
import os
import tempfile
from urllib.parse import unquote_plus

import boto3

from src.search_keyword_analyzer import SearchKeywordAnalyzer

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "output/")


def handler(event: dict, context: object) -> dict:
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
            output_key = OUTPUT_PREFIX + output_path.name
            s3.upload_file(str(output_path), bucket, output_key)
            logger.info("Uploaded results to s3://%s/%s", bucket, output_key)

    return {"statusCode": 200, "body": "OK"}
