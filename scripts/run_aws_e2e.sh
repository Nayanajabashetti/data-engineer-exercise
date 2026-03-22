#!/usr/bin/env bash
# One command: Glue E2E (upload → job → verify S3) + Lambda invoke timing.
#
# Setup once (repo root):
#   cp .env.aws.example .env.aws
#   # edit DATA_BUCKET (and AWS_REGION if needed)
#
# Run:
#   ./scripts/run_aws_e2e.sh
#
# Optional:
#   SKIP_GLUE=1   — only Lambda timing (seconds)
#   SKIP_LAMBDA=1 — only Glue E2E (~minutes)

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "${ROOT}"

# Preserve values already in the environment (exported before this script).
# Sourcing .env.aws with DATA_BUCKET= (empty) would otherwise wipe them.
_FROM_ENV_AWS_REGION="${AWS_REGION-}"
_FROM_ENV_DATA_BUCKET="${DATA_BUCKET-}"

if [[ -f "${ROOT}/.env.aws" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "${ROOT}/.env.aws"
  set +a
fi

if [[ -z "${AWS_REGION}" && -n "${_FROM_ENV_AWS_REGION}" ]]; then
  AWS_REGION="${_FROM_ENV_AWS_REGION}"
fi
if [[ -z "${DATA_BUCKET}" && -n "${_FROM_ENV_DATA_BUCKET}" ]]; then
  DATA_BUCKET="${_FROM_ENV_DATA_BUCKET}"
fi

: "${AWS_REGION:?Set AWS_REGION in .env.aws or: export AWS_REGION=us-west-2}"
: "${DATA_BUCKET:?Set DATA_BUCKET in .env.aws (not empty) or: export DATA_BUCKET=your-bucket}"

if [[ -z "${DATA_BUCKET}" ]] || [[ "${DATA_BUCKET}" == "your-bucket-name" ]]; then
  echo "ERROR: DATA_BUCKET is missing or still the example placeholder."
  echo "  Put your bucket name on the DATA_BUCKET= line in .env.aws (copy from Terraform):"
  echo "    echo \"DATA_BUCKET=\$(cd terraform && terraform output -raw s3_bucket_name)\" >> .env.aws"
  echo "  Or run once:"
  echo "    export DATA_BUCKET=\$(cd terraform && terraform output -raw s3_bucket_name)"
  echo "  Note: DATA_BUCKET=value must be export-ed, or set in .env.aws — a blank DATA_BUCKET= in .env.aws clears exports."
  exit 1
fi

export AWS_REGION
export DATA_BUCKET

export LANDING_PREFIX="${LANDING_PREFIX:-landing/}"
export CURATED_PREFIX="${CURATED_PREFIX:-curated/search_keyword/}"
export INPUT_PREFIX="${INPUT_PREFIX:-${LANDING_PREFIX}}"

if [[ "${SKIP_GLUE:-}" != "1" ]]; then
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo " 1/2  Glue: upload → job → verify curated output"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  "${ROOT}/scripts/e2e_aws.sh"
else
  echo "SKIP_GLUE=1 — skipping Glue (set SKIP_GLUE=0 or unset to run Glue)."
fi

if [[ "${SKIP_LAMBDA:-}" != "1" ]]; then
  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo " 2/2  Lambda: upload → invoke → REPORT (Duration ms)"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  "${ROOT}/scripts/lambda_timing_aws.sh"
else
  echo "SKIP_LAMBDA=1 — skipping Lambda."
fi

echo ""
echo "=== Done: full AWS E2E path (Glue + Lambda) ==="
