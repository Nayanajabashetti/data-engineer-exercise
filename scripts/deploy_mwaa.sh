#!/usr/bin/env bash
# One-shot: sync local DAGs (and packaged assets) to the S3 prefix used by Amazon MWAA.
#
# Prerequisites:
#   - MWAA enabled via Terraform (enable_mwaa=true) and environment AVAILABLE
#   - DATA_BUCKET matches terraform bucket_name
#
# Usage (repo root):
#   export DATA_BUCKET=your-bucket-name
#   ./scripts/deploy_mwaa.sh
#
# Does NOT run terraform apply. For infra + requirements.txt in S3:
#   cd terraform && terraform apply
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

if [[ -f "${ROOT}/.env.aws" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "${ROOT}/.env.aws"
  set +a
fi

: "${DATA_BUCKET:?Set DATA_BUCKET to your S3 data bucket (same as terraform bucket_name)}"

echo "==> Sync DAGs to s3://${DATA_BUCKET}/ (see MWAA output mwaa_dag_s3_uri)"
"${ROOT}/scripts/sync_airflow_dags_to_s3.sh"

echo ""
echo "==> Next steps"
echo "  1) Open Airflow UI:  cd terraform && terraform output -raw mwaa_webserver_url"
echo "  2) Admin → Variables: set search_keyword_bucket=${DATA_BUCKET}"
echo "  3) Admin → Connections → aws_default: Extra JSON {\"region_name\": \"<your-region>\"}"
echo "  4) Enable DAG search_keyword_glue_pipeline and trigger a run."
echo ""
echo "If you changed airflow/requirements.txt, run:  cd terraform && terraform apply"
echo "Then update the MWAA environment in the AWS console so workers reinstall packages."
