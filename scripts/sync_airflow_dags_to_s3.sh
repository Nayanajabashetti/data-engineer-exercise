#!/usr/bin/env bash
# Upload local Airflow DAGs to the S3 prefix used by Amazon MWAA.
#
# Usage:
#   export DATA_BUCKET=your-bucket-name
#   export MWAA_DAGS_PREFIX=airflow/dags   # no trailing slash
#   ./scripts/sync_airflow_dags_to_s3.sh
#
set -euo pipefail

: "${DATA_BUCKET:?Set DATA_BUCKET}"
MWAA_DAGS_PREFIX="${MWAA_DAGS_PREFIX:-airflow/dags}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "==> Sync ${ROOT}/airflow/dags/ -> s3://${DATA_BUCKET}/${MWAA_DAGS_PREFIX}/"
aws s3 sync "${ROOT}/airflow/dags/" "s3://${DATA_BUCKET}/${MWAA_DAGS_PREFIX}/" \
  --exclude "__pycache__/*" \
  --exclude "*.pyc"

echo "OK. MWAA will pick up changes within a few minutes (or trigger 'Update environment' if needed)."
