#!/usr/bin/env bash
# Run MWAA validator (permissions + paths + SSM), then sync local airflow/dags/ to S3.
# Use before opening the Airflow UI.
#
# Required env:
#   MWAA_ENV_NAME   — from: aws mwaa list-environments
#   DATA_BUCKET     — e.g. acs-keyword-revenue-nayanaj (must match MWAA source bucket for sync)
# Optional:
#   AWS_REGION      — default us-west-2
#   MWAA_DAGS_PREFIX — default airflow/dags
#
# Usage:
#   export MWAA_ENV_NAME=your-mwaa-environment-name
#   export DATA_BUCKET=acs-keyword-revenue-nayanaj
#   export AWS_REGION=us-west-2
#   ./scripts/mwaa_preflight_and_sync.sh
#
# See also: docs/mwaa_ship_and_run.md (Configure → preflight & sync → run)
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

: "${MWAA_ENV_NAME:?Set MWAA_ENV_NAME}"
: "${DATA_BUCKET:?Set DATA_BUCKET}"

echo "=== Step 1/2: Validator (MWAA + SSM) ==="
"${ROOT}/scripts/verify_mwaa_ready.sh"

echo ""
echo "=== Step 2/2: Sync DAGs to s3://${DATA_BUCKET}/${MWAA_DAGS_PREFIX:-airflow/dags}/ ==="
export MWAA_DAGS_PREFIX="${MWAA_DAGS_PREFIX:-airflow/dags}"
"${ROOT}/scripts/sync_airflow_dags_to_s3.sh"

echo ""
echo "=== Done ==="
echo "If SSM check passed, you can skip Airflow Admin → Variables for search_keyword_bucket."
echo "Open MWAA UI → DAG search_keyword_glue_pipeline → Unpause → Trigger."
