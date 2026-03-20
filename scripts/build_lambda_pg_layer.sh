#!/usr/bin/env bash
# Build a Lambda layer zip with pg8000 (Linux-compatible wheels for Python 3.12).
# Run before a demo if the function zip from Terraform does not bundle dependencies.
#
# Usage:
#   ./scripts/build_lambda_pg_layer.sh
# Outputs:
#   .lambda-pg-layer/python/   (gitignored)
#   .lambda-pg-layer.zip       (gitignored)
#
# Then publish and attach (replace REGION / ACCOUNT):
#   aws lambda publish-layer-version --layer-name search-keyword-pg8000 \
#     --zip-file fileb://.lambda-pg-layer.zip --compatible-runtimes python3.12 --region us-west-2
#   aws lambda update-function-configuration --function-name search-keyword-performance \
#     --layers <LayerVersionArn> --region us-west-2

set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${ROOT}/.lambda-pg-layer"
ZIP="${ROOT}/.lambda-pg-layer.zip"

rm -rf "${OUT}" "${ZIP}"
mkdir -p "${OUT}/python"

if command -v docker >/dev/null 2>&1; then
  echo "Installing pg8000 into layer using Lambda Python 3.12 image (recommended on macOS)..."
  docker run --rm --platform linux/amd64 \
    -v "${OUT}/python:/opt/python" \
    public.ecr.aws/lambda/python:3.12 \
    /bin/bash -c "pip install --no-cache-dir -t /opt/python 'pg8000==1.31.5'"
else
  echo "Docker not found; installing with pip (use Linux CI or Docker if Lambda fails to import pg8000)..."
  python3 -m pip install --no-cache-dir -t "${OUT}/python" "pg8000==1.31.5" \
    --platform manylinux2014_x86_64 --python-version 3.12 --only-binary=:all: 2>/dev/null \
    || python3 -m pip install --no-cache-dir -t "${OUT}/python" "pg8000==1.31.5"
fi

( cd "${OUT}" && zip -qr "${ZIP}" python )
echo "Created ${ZIP}"
ls -lh "${ZIP}"
