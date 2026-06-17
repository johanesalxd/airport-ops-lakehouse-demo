#!/usr/bin/env bash
#
# Generate synthetic data and upload it to the raw GCS landing zone, preserving
# the dt=YYYY-MM-DD partition layout.
#
# Usage:
#   source .env && bash scripts/upload_demo_data.sh [DAYS] [SEED]
#
set -euo pipefail
: "${RAW_BUCKET:=}"

if [[ -z "${RAW_BUCKET}" ]]; then
  : "${PROJECT_ID:?set PROJECT_ID (source .env)}"
  PROJECT_NUMBER="${PROJECT_NUMBER:-$(
    gcloud projects describe "${PROJECT_ID}" --format='value(projectNumber)'
  )}"
  RAW_BUCKET="airport-ops-demo-${PROJECT_NUMBER}"
fi

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

# Start from a clean remote landing zone so reruns are deterministic. Without
# this, older dt= partitions from longer prior runs can remain and be ingested.
echo ">> Cleaning remote landing zone gs://${RAW_BUCKET}/raw"
gcloud storage rm -r "gs://${RAW_BUCKET}/raw" 2>/dev/null \
  && echo "   cleared existing raw objects" \
  || echo "   (no existing raw objects)"

echo ">> Uploading to gs://${RAW_BUCKET}/raw/"
gcloud storage rsync -r "${OUT_DIR}" "gs://${RAW_BUCKET}/raw"

echo ">> Upload complete. Layout:"
gcloud storage ls -r "gs://${RAW_BUCKET}/raw" | head -20
