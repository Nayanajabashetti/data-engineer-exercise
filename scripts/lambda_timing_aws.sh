#!/usr/bin/env bash
# Time the search-keyword-performance Lambda (same path as S3 trigger: read object, process, write curated).
#
# 1) Uploads sample_hit_data.parquet (from TSV) under INPUT_PREFIX
# 2) Invokes Lambda synchronously with an S3 event payload pointing at that key
# 3) Prints REPORT line (includes Duration ms) from the response log tail
#
# Usage:
#   export AWS_REGION=us-west-2
#   export DATA_BUCKET=your-bucket
#   ./scripts/lambda_timing_aws.sh
#
# Optional:
#   LAMBDA_NAME=search-keyword-performance
#   INPUT_PREFIX=input/          # must match Terraform input_prefix / S3 notification filter
#   SAMPLE=sample_hit_data.tsv (converted to Parquet before upload)

set -euo pipefail

: "${AWS_REGION:?Set AWS_REGION}"
: "${DATA_BUCKET:?Set DATA_BUCKET}"

LAMBDA_NAME="${LAMBDA_NAME:-search-keyword-performance}"
INPUT_PREFIX="${INPUT_PREFIX:-landing/}"
SAMPLE="${SAMPLE:-sample_hit_data.tsv}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

if ! python3 -c "import pyarrow" 2>/dev/null; then
  echo "ERROR: PyArrow is required to convert the sample TSV to Parquet."
  echo "  python3 -m pip install -r ${ROOT}/requirements.txt"
  exit 1
fi

PARQUET_NAME="${SAMPLE%.tsv}.parquet"
TMP_PQ="${ROOT}/.lambda_timing_${PARQUET_NAME}"
python3 "${ROOT}/scripts/tsv_to_parquet_landing.py" "${ROOT}/${SAMPLE}" "${TMP_PQ}"
KEY="${INPUT_PREFIX%/}/lambda_timing_$(date -u +%Y%m%d%H%M%S)_${PARQUET_NAME}"
PAYLOAD="$(mktemp)"
RESP="$(mktemp)"
META_FILE="$(mktemp)"

cleanup() { rm -f "${PAYLOAD}" "${RESP}" "${META_FILE}"; }
trap cleanup EXIT

echo "==> Upload s3://${DATA_BUCKET}/${KEY}"
aws s3 cp "${TMP_PQ}" "s3://${DATA_BUCKET}/${KEY}" --region "${AWS_REGION}"

cat > "${PAYLOAD}" <<EOF
{
  "Records": [
    {
      "eventVersion": "2.1",
      "eventSource": "aws:s3",
      "awsRegion": "${AWS_REGION}",
      "eventName": "ObjectCreated:Put",
      "s3": {
        "bucket": { "name": "${DATA_BUCKET}" },
        "object": { "key": "${KEY}" }
      }
    }
  ]
}
EOF

echo "==> Invoke ${LAMBDA_NAME} (sync) and fetch log tail..."
# Response payload -> ${RESP}; metadata (LogResult, StatusCode) -> stdout, captured here
START=$(date +%s)
aws lambda invoke \
  --function-name "${LAMBDA_NAME}" \
  --region "${AWS_REGION}" \
  --cli-binary-format raw-in-base64-out \
  --payload "file://${PAYLOAD}" \
  --log-type Tail \
  "${RESP}" > "${META_FILE}"
END=$(date +%s)
WALL=$((END - START))

LOG_B64=$(jq -r '.LogResult // empty' "${META_FILE}" 2>/dev/null || true)
if [[ -n "${LOG_B64}" ]]; then
  echo ""
  echo "----- CloudWatch tail (Lambda) -----"
  echo "${LOG_B64}" | base64 -d 2>/dev/null || echo "${LOG_B64}"
  echo "-----"
fi

echo ""
echo "==> Wall-clock for aws lambda invoke: ${WALL}s"
echo "==> Lambda response body:"
cat "${RESP}"
echo ""

ERR=$(jq -r '.FunctionError // empty' "${META_FILE}" 2>/dev/null || true)
if [[ "${ERR}" == "Unhandled" ]] || [[ "${ERR}" == "Handled" ]]; then
  echo "WARN: FunctionError=${ERR} — check response body and logs above."
  echo "    (REPORT line still shows billed Duration if the handler ran far enough.)"
  echo "    If SYNC_DB_SINKS=true, DB/RDS errors can fail the invoke even when analysis is fast."
  exit 1
fi

echo "OK. Compare 'Duration' / 'Billed Duration' in REPORT above vs Glue (~60–90s wall-clock)."
