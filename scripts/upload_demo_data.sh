#!/usr/bin/env bash
#
# Generate synthetic data and upload it to the raw GCS landing zone, preserving
# the dt=YYYY-MM-DD partition layout.
#
# Usage:
#   source .env && bash scripts/upload_demo_data.sh [DAYS] [SEED]
#
set -euo pipefail
: "${RAW_BUCKET:?set RAW_BUCKET (source .env)}"

DAYS="${1:-3}"
SEED="${2:-42}"
OUT_DIR="./out"

echo ">> Generating ${DAYS} day(s) of synthetic data (seed=${SEED})"
python3 scripts/generate_demo_data.py --out-dir "${OUT_DIR}" --days "${DAYS}" --seed "${SEED}"

echo ">> Uploading to gs://${RAW_BUCKET}/raw/"
gcloud storage rsync -r "${OUT_DIR}" "gs://${RAW_BUCKET}/raw" 

echo ">> Upload complete. Layout:"
gcloud storage ls -r "gs://${RAW_BUCKET}/raw" | head -20
