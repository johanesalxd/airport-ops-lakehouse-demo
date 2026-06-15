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

# Clean up stale per-source files from earlier formats so the landing zone always
# matches the current generator output. rsync (below) does not delete extras, and
# the customer_feedback external table globs *.jsonl -- a leftover *.json from a
# previous run would just be dead clutter. Add more patterns here if a source's
# format changes again.
echo ">> Cleaning stale objects in landing zone"
gcloud storage rm "gs://${RAW_BUCKET}/raw/customer_feedback/**/customer_feedback.json" 2>/dev/null \
  && echo "   removed stale customer_feedback.json" \
  || echo "   (no stale customer_feedback.json)"

echo ">> Uploading to gs://${RAW_BUCKET}/raw/"
gcloud storage rsync -r "${OUT_DIR}" "gs://${RAW_BUCKET}/raw" 

echo ">> Upload complete. Layout:"
gcloud storage ls -r "gs://${RAW_BUCKET}/raw" | head -20
