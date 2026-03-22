#!/usr/bin/env bash
# Preflight: confirm MWAA env + SSM data-bucket parameter before syncing DAGs to S3.
# Run this first to catch IAM/region/path issues (same checks as the first half of mwaa_preflight_and_sync.sh).
#
# Usage:
#   export MWAA_ENV_NAME=search-keyword-airflow   # your environment name
#   export AWS_REGION=us-west-2
#   ./scripts/verify_mwaa_ready.sh
#
set -euo pipefail

: "${MWAA_ENV_NAME:?Set MWAA_ENV_NAME (see: aws mwaa list-environments)}"
REGION="${AWS_REGION:-us-west-2}"
SSM_NAME="${SSM_DATA_BUCKET_PARAM:-/search-keyword-performance/airflow/data_bucket_name}"

echo "==> MWAA environment: ${MWAA_ENV_NAME} (${REGION})"
aws mwaa get-environment --name "${MWAA_ENV_NAME}" --region "${REGION}" \
  --query 'Environment.{SourceBucketArn:SourceBucketArn,DagS3Path:DagS3Path,Status:Status}' \
  --output table

echo ""
echo "==> SSM parameter (DAG uses this when Variable search_keyword_bucket is unset): ${SSM_NAME}"
if aws ssm get-parameter --name "${SSM_NAME}" --region "${REGION}" --query 'Parameter.Value' --output text 2>/dev/null; then
  echo "OK: SSM parameter exists."
else
  echo "WARN: Parameter missing or no access. Set Airflow Variable search_keyword_bucket or apply Terraform with enable_mwaa=true."
  exit 1
fi

echo ""
echo "==> Next: export DATA_BUCKET=<your data bucket> && ./scripts/sync_airflow_dags_to_s3.sh"
echo "    Then open MWAA UI → DAG search_keyword_glue_pipeline → Unpause → Trigger."
