#!/usr/bin/env bash
# Run Terraform from repo root context: init + plan or apply.
#
# Config (pick one):
#   A) terraform/terraform.tfvars  (copy from terraform.tfvars.example) — full control
#   B) Repo root .env.aws with DATA_BUCKET=... and AWS_REGION=... — sets TF_VAR_* for minimal apply
#
# Usage:
#   ./scripts/terraform_apply.sh plan              # show changes only
#   ./scripts/terraform_apply.sh apply             # interactive approve
#   TF_APPLY_AUTO_APPROVE=1 ./scripts/terraform_apply.sh apply   # no prompt
#   ./scripts/terraform_apply.sh apply --yes       # same as AUTO_APPROVE=1
#
# Extra args are passed to terraform (e.g. -target=...):
#   ./scripts/terraform_apply.sh apply -var='glue_worker_count=4'
#
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TF_DIR="${ROOT}/terraform"
cd "${TF_DIR}"

if [[ -f "${ROOT}/.env.aws" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "${ROOT}/.env.aws"
  set +a
fi

# Minimal vars when no terraform.tfvars (optional file — gitignored)
if [[ ! -f "${TF_DIR}/terraform.tfvars" ]]; then
  : "${DATA_BUCKET:?Set DATA_BUCKET in .env.aws or create terraform/terraform.tfvars}"
  if [[ -z "${DATA_BUCKET}" || "${DATA_BUCKET}" == "your-bucket-name" ]]; then
    echo "ERROR: DATA_BUCKET is missing or still the placeholder in .env.aws"
    exit 1
  fi
  export TF_VAR_bucket_name="${DATA_BUCKET}"
  export TF_VAR_aws_region="${AWS_REGION:-us-west-2}"
fi

terraform init -upgrade

MODE="${1:-apply}"
shift || true

AUTO=0
if [[ "${TF_APPLY_AUTO_APPROVE:-0}" == "1" ]] || [[ "${1:-}" == "--yes" ]]; then
  AUTO=1
  [[ "${1:-}" == "--yes" ]] && shift
fi

case "${MODE}" in
  plan)
    terraform plan "$@"
    ;;
  apply)
    if [[ "${AUTO}" -eq 1 ]]; then
      terraform apply -auto-approve "$@"
    else
      terraform apply "$@"
    fi
    ;;
  init)
    echo "Already ran init. Use: $0 plan | apply"
    ;;
  *)
    echo "Usage: $0 plan|apply [--yes] [extra terraform args]"
    exit 1
    ;;
esac
