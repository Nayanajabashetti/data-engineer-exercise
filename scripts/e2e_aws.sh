#!/usr/bin/env bash
# End-to-end smoke test against AWS (Glue + S3 curated output).
#
# Prerequisites: AWS CLI configured; Terraform already applied; Glue job + bucket exist.
#
# Usage:
#   export AWS_REGION=us-west-2
#   export DATA_BUCKET=your-bucket-name
#   ./scripts/e2e_aws.sh
#
# Optional (must match Glue job --input_path / --output_path prefixes):
#   LANDING_PREFIX=landing/   # Terraform default; legacy stacks often use input/
#   CURATED_PREFIX=curated/search_keyword/   # Terraform default; legacy often output/
#   GLUE_JOB=search-keyword-performance
#
# Example if your job still uses input/ + output/:
#   LANDING_PREFIX=input/ CURATED_PREFIX=output/ ./scripts/e2e_aws.sh

set -euo pipefail

: "${AWS_REGION:?Set AWS_REGION}"
: "${DATA_BUCKET:?Set DATA_BUCKET (S3 data bucket name)}"

LANDING_PREFIX="${LANDING_PREFIX:-landing/}"
CURATED_PREFIX="${CURATED_PREFIX:-curated/search_keyword/}"
GLUE_JOB="${GLUE_JOB:-search-keyword-performance}"
SAMPLE="${SAMPLE:-sample_hit_data.tsv}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

if ! python3 -c "import pyarrow" 2>/dev/null; then
  echo "ERROR: PyArrow is required to convert the sample TSV to Parquet for Glue."
  echo "  python3 -m pip install -r ${ROOT}/requirements.txt"
  echo "  # or: python3 -m pip install pyarrow"
  exit 1
fi

TODAY_UTC="$(date -u +%F)"
PARQUET_NAME="${SAMPLE%.tsv}.parquet"
TMP_PQ="${ROOT}/.e2e_${PARQUET_NAME}"
# Glue / landing default is Parquet (see --landing_format); upload Parquet derived from the TSV sample.
TS_KEY="${LANDING_PREFIX%/}/dt=${TODAY_UTC}/e2e_$(date -u +%Y%m%d%H%M%S)_${PARQUET_NAME}"

echo "==> Convert ${SAMPLE} -> Parquet (${TMP_PQ})"
python3 "${ROOT}/scripts/tsv_to_parquet_landing.py" "${ROOT}/${SAMPLE}" "${TMP_PQ}"

echo "==> Upload sample to s3://${DATA_BUCKET}/${TS_KEY}"
aws s3 cp "${TMP_PQ}" "s3://${DATA_BUCKET}/${TS_KEY}" --region "${AWS_REGION}"
PI="${PARTITION_INTERVAL_MINUTES:-15}"
echo "==> Start Glue job ${GLUE_JOB} (partition_dt=${TODAY_UTC}, partition_interval_minutes=${PI})"
# Extra args merge with the job's default_arguments (input_path, output_path, etc.).
RUN_ID="$(
  aws glue start-job-run \
    --job-name "${GLUE_JOB}" \
    --region "${AWS_REGION}" \
    --arguments "{\"--partition_dt\":\"${TODAY_UTC}\",\"--partition_interval_minutes\":\"${PI}\"}" \
    --query 'JobRunId' \
    --output text
)"

echo "==> Glue run id: ${RUN_ID}"
echo "==> Waiting for Glue (poll every 30s, max ~60 min)..."
for _ in $(seq 1 120); do
  STATE="$(aws glue get-job-run --job-name "${GLUE_JOB}" --run-id "${RUN_ID}" --region "${AWS_REGION}" --query 'JobRun.JobRunState' --output text)"
  echo "    state=${STATE}"
  if [[ "${STATE}" == "SUCCEEDED" ]]; then
    break
  fi
  if [[ "${STATE}" == "FAILED" ]] || [[ "${STATE}" == "STOPPED" ]] || [[ "${STATE}" == "TIMEOUT" ]]; then
    echo "Glue failed. Check CloudWatch logs for the job run."
    aws glue get-job-run --job-name "${GLUE_JOB}" --run-id "${RUN_ID}" --region "${AWS_REGION}" --output json
    exit 1
  fi
  sleep 30
done

NEEDLE="${TODAY_UTC}_SearchKeywordPerformance"
echo "==> Look for curated output containing ${NEEDLE}"
# Avoid pipefail / aws s3 ls exit-code quirks: capture keys then grep fixed-string.
KEYS="$(
  aws s3 ls "s3://${DATA_BUCKET}/${CURATED_PREFIX}" --recursive --region "${AWS_REGION}" 2>/dev/null || true
)"
if echo "${KEYS}" | grep -Fq "${NEEDLE}"; then
  echo "OK: Found curated output under s3://${DATA_BUCKET}/${CURATED_PREFIX}"
else
  echo "WARN: No object key containing '${NEEDLE}'. Recent keys:"
  echo "${KEYS}" | tail -20 || true
  exit 1
fi

echo "==> E2E Glue path: SUCCESS"
