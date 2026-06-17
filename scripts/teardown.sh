#!/usr/bin/env bash
#
# Teardown for the Airport Operations Lakehouse demo. Removes the demo datasets,
# raw bucket contents, and Pub/Sub streaming resources.
#
# NOT a full inverse of bootstrap.sh (see docs/roadmap.md "Public-release prep"):
#   - It does NOT delete the Dataform service account (dataform-airport@…) and
#     revokes none of the IAM bindings bootstrap grants.
#   - Shared connections (spark-etl-conn, gemini_conn, default-us-central1) are
#     left intact by design.
#   - The GCP Dataform repository is user-managed and is left intact.
#
# Usage:
#   source .env && bash scripts/teardown.sh
#
set -euo pipefail
: "${PROJECT_ID:?set PROJECT_ID (source .env)}"
: "${REGION:?set REGION}"
: "${DS_BRONZE:?set DS_BRONZE}"
: "${DS_SILVER:?set DS_SILVER}"
: "${DS_GOLD:?set DS_GOLD}"
: "${DS_SEMANTIC:?set DS_SEMANTIC}"
: "${DS_AI:?set DS_AI}"
: "${DS_CONTROL:?set DS_CONTROL}"
: "${DS_ASSERTIONS:?set DS_ASSERTIONS}"
: "${DS_GOVERNANCE:?set DS_GOVERNANCE}"
: "${PROJECT_NUMBER:=}"
: "${RAW_BUCKET:=}"
: "${PUBSUB_BAGGAGE_SCHEMA:=baggage-scan-event}"
: "${PUBSUB_BAGGAGE_TOPIC:=baggage-events}"
: "${PUBSUB_BAGGAGE_DLQ_TOPIC:=baggage-events-dlq}"
: "${PUBSUB_BAGGAGE_SUBSCRIPTION:=baggage-events-bq-sub}"

if [[ -z "${PROJECT_NUMBER}" ]]; then
  PROJECT_NUMBER="$(gcloud projects describe "${PROJECT_ID}" --format='value(projectNumber)')"
fi
if [[ -z "${RAW_BUCKET}" ]]; then
  RAW_BUCKET="airport-ops-demo-${PROJECT_NUMBER}"
fi

read -r -p "This will DELETE demo datasets, bucket data, and Pub/Sub resources. Continue? [y/N] " ans
[[ "${ans}" == "y" || "${ans}" == "Y" ]] || { echo "Aborted."; exit 1; }

for DS in "${DS_BRONZE}" "${DS_SILVER}" "${DS_GOLD}" "${DS_SEMANTIC}" \
          "${DS_AI}" "${DS_CONTROL}" "${DS_ASSERTIONS}" "${DS_GOVERNANCE}"; do
  bq --location="${REGION}" rm -r -f -d "${PROJECT_ID}:${DS}" 2>/dev/null \
    && echo "deleted dataset ${DS}" || echo "skip ${DS}"
done

gcloud storage rm -r "gs://${RAW_BUCKET}/raw" 2>/dev/null \
  && echo "cleared raw bucket" || echo "skip bucket"

gcloud pubsub subscriptions delete "${PUBSUB_BAGGAGE_SUBSCRIPTION}" --quiet \
  2>/dev/null && echo "deleted subscription ${PUBSUB_BAGGAGE_SUBSCRIPTION}" \
  || echo "skip subscription ${PUBSUB_BAGGAGE_SUBSCRIPTION}"
gcloud pubsub topics delete "${PUBSUB_BAGGAGE_TOPIC}" --quiet 2>/dev/null \
  && echo "deleted topic ${PUBSUB_BAGGAGE_TOPIC}" \
  || echo "skip topic ${PUBSUB_BAGGAGE_TOPIC}"
gcloud pubsub topics delete "${PUBSUB_BAGGAGE_DLQ_TOPIC}" --quiet 2>/dev/null \
  && echo "deleted topic ${PUBSUB_BAGGAGE_DLQ_TOPIC}" \
  || echo "skip topic ${PUBSUB_BAGGAGE_DLQ_TOPIC}"
gcloud pubsub schemas delete "${PUBSUB_BAGGAGE_SCHEMA}" --quiet 2>/dev/null \
  && echo "deleted schema ${PUBSUB_BAGGAGE_SCHEMA}" \
  || echo "skip schema ${PUBSUB_BAGGAGE_SCHEMA}"

# Note: deleting the governance dataset removes staff_directory and its row
# access policies, but the region-scoped DATA_POLICYs (staff_*_policy under
# region-${REGION}) are not tied to the dataset and persist. They are harmless
# (cls_staff_directory.sqlx uses CREATE OR REPLACE, so a rebuild is idempotent).
# To remove them explicitly, see the BigQuery data policies API / DROP DATA POLICY.
echo "note: region-scoped staff_*_policy data policies persist (idempotent on rebuild)"

echo "kept user-managed Dataform repo"

echo "Teardown done. Shared connections and Composer were left intact."
