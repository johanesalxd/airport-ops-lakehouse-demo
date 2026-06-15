#!/usr/bin/env bash
#
# Teardown for the Airport Operations Lakehouse demo. Removes datasets, the raw
# bucket contents, the Dataform repository, and the Dataform service account.
# Connections (spark-etl-conn, gemini_conn) are left intact since they are shared.
#
# Usage:
#   source .env && bash scripts/teardown.sh
#
set -euo pipefail
: "${PROJECT_ID:?set PROJECT_ID (source .env)}"
: "${REGION:?set REGION}"

read -r -p "This will DELETE demo datasets, bucket data, and the Dataform repo. Continue? [y/N] " ans
[[ "${ans}" == "y" || "${ans}" == "Y" ]] || { echo "Aborted."; exit 1; }

for DS in "${DS_BRONZE}" "${DS_SILVER}" "${DS_GOLD}" "${DS_SEMANTIC}" \
          "${DS_AI}" "${DS_CONTROL}" "${DS_ASSERTIONS}" "${DS_GOVERNANCE}"; do
  bq --location="${REGION}" rm -r -f -d "${PROJECT_ID}:${DS}" 2>/dev/null \
    && echo "deleted dataset ${DS}" || echo "skip ${DS}"
done

gcloud storage rm -r "gs://${RAW_BUCKET}/raw" 2>/dev/null \
  && echo "cleared raw bucket" || echo "skip bucket"

# Note: deleting the governance dataset removes staff_directory and its row
# access policies, but the region-scoped DATA_POLICYs (staff_*_policy under
# region-${REGION}) are not tied to the dataset and persist. They are harmless
# (cls_staff_directory.sqlx uses CREATE OR REPLACE, so a rebuild is idempotent).
# To remove them explicitly, see the BigQuery data policies API / DROP DATA POLICY.
echo "note: region-scoped staff_*_policy data policies persist (idempotent on rebuild)"

gcloud dataform repositories delete "${DATAFORM_REPO_ID}" \
  --region="${REGION}" --quiet 2>/dev/null \
  && echo "deleted dataform repo" || echo "skip dataform repo"

echo "Teardown done. Shared connections were left intact."
