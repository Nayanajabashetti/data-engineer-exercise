#!/usr/bin/env bash
# Full deploy helper: Terraform apply + optional Airflow DAG sync to S3.
#
# Prerequisites: AWS CLI credentials, Docker recommended for Lambda Linux wheels (see README).
#
# Usage (repo root):
#   ./scripts/deploy_aws.sh                    # terraform apply (interactive), then sync DAGs if SYNC_AIRFLOW_DAGS=1
#   SYNC_AIRFLOW_DAGS=1 ./scripts/deploy_aws.sh
#   TF_APPLY_AUTO_APPROVE=1 ./scripts/deploy_aws.sh
#
# With terraform.tfvars (preferred for MWAA — set enable_mwaa, vpc, subnets there):
#   cd terraform && cp terraform.tfvars.example terraform.tfvars   # edit
#   cd .. && SYNC_AIRFLOW_DAGS=1 ./scripts/deploy_aws.sh
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

echo "==> Terraform (init + apply)"
AUTO_FLAG=()
if [[ "${TF_APPLY_AUTO_APPROVE:-0}" == "1" ]] || [[ "${1:-}" == "--yes" ]]; then
  AUTO_FLAG=(--yes)
  [[ "${1:-}" == "--yes" ]] && shift
fi

# shellcheck disable=SC2086
"${ROOT}/scripts/terraform_apply.sh" apply "${AUTO_FLAG[@]}" "$@"

if [[ "${SYNC_AIRFLOW_DAGS:-0}" == "1" ]]; then
  echo ""
  echo "==> Sync Airflow DAGs to S3 (MWAA)"
  if [[ -z "${DATA_BUCKET:-}" ]]; then
    DATA_BUCKET="$(cd "${ROOT}/terraform" && terraform output -raw s3_bucket_name 2>/dev/null || true)"
  fi
  : "${DATA_BUCKET:?Set DATA_BUCKET in .env.aws or ensure terraform output s3_bucket_name works}"
  export DATA_BUCKET
  "${ROOT}/scripts/deploy_mwaa.sh"
else
  echo ""
  echo "Skip DAG sync (set SYNC_AIRFLOW_DAGS=1 to run scripts/deploy_mwaa.sh after apply)."
fi

echo ""
echo "==> Done. Outputs:"
(cd "${ROOT}/terraform" && terraform output -compact-warnings 2>/dev/null || true)
