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

# Start from a clean local staging dir so leftover files from an earlier format
# (e.g. customer_feedback.json before the switch to .jsonl) are not re-uploaded by
# rsync, which does not delete extras at the destination.
echo ">> Resetting local staging dir ${OUT_DIR}"
rm -rf "${OUT_DIR}"

echo ">> Generating ${DAYS} day(s) of synthetic data (seed=${SEED})"
# Run via uv so Python + pyarrow are pinned/reproducible (see pyproject.toml).
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
uv run --project "${REPO_ROOT}" python "${REPO_ROOT}/scripts/generate_demo_data.py" \
  --out-dir "${OUT_DIR}" --days "${DAYS}" --seed "${SEED}"

# Also remove matching stale objects already in the GCS landing zone from prior
# uploads (rsync only adds/updates, never deletes). Add more patterns here if a
# source's format changes again.
echo ">> Cleaning stale objects in landing zone"
gcloud storage rm "gs://${RAW_BUCKET}/raw/customer_feedback/**/customer_feedback.json" 2>/dev/null \
  && echo "   removed stale customer_feedback.json" \
  || echo "   (no stale customer_feedback.json)"

echo ">> Uploading to gs://${RAW_BUCKET}/raw/"
gcloud storage rsync -r "${OUT_DIR}" "gs://${RAW_BUCKET}/raw" 

echo ">> Upload complete. Layout:"
gcloud storage ls -r "gs://${RAW_BUCKET}/raw" | head -20
