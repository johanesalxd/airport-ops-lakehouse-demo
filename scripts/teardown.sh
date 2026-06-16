#!/usr/bin/env bash
#
# Teardown for the Airport Operations Lakehouse demo. Removes the 8 datasets and
# the raw bucket contents, and deletes the GCP Dataform repository.
#
# NOT a full inverse of bootstrap.sh (see docs/roadmap.md "Public-release prep"):
#   - It does NOT delete the Dataform service account (dataform-airport@…) and
#     revokes none of the IAM bindings bootstrap grants.
#   - Shared connections (spark-etl-conn, gemini_conn, default-us-central1) are
#     left intact by design.
#   - The GCP Dataform repository is treated as pre-provisioned (reused, not
#     created by bootstrap.sh). Deleting it here means a later teardown→bootstrap
#     cycle requires re-provisioning the repository + its Git connection manually
#     before the next run. Comment out the delete below to keep it.
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
