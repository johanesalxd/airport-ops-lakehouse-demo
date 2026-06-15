#!/usr/bin/env bash
#
# Idempotent provisioning for the Airport Operations Lakehouse demo.
# Creates datasets, the raw GCS bucket, a Dataform service account, grants
# the IAM required for serverless Spark stored procedures, Gemini remote models,
# Dataform execution, and Composer orchestration, and uploads the Composer DAG.
#
# Reuses existing connections (spark-etl-conn, gemini_conn). It does NOT create
# BigQuery connections.
#
# Usage:
#   cp .env.example .env && nano .env
#   source .env && bash scripts/bootstrap.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DAG_FILE="${REPO_ROOT}/composer/dags/airport_ops_lakehouse_dag.py"

: "${PROJECT_ID:?set PROJECT_ID (source .env)}"
: "${REGION:?set REGION}"
: "${RAW_BUCKET:?set RAW_BUCKET}"

echo ">> Project: ${PROJECT_ID}  Region: ${REGION}"
gcloud config set project "${PROJECT_ID}" >/dev/null

echo ">> Enabling required APIs (no-op if already enabled)"
# dataplex + cloudaicompanion (Gemini for Google Cloud) power the optional
# data-insights script (scripts/generate_data_insights.sh).
gcloud services enable \
  bigquery.googleapis.com bigqueryconnection.googleapis.com \
  dataform.googleapis.com dataproc.googleapis.com \
  aiplatform.googleapis.com composer.googleapis.com \
  secretmanager.googleapis.com storage.googleapis.com \
  dataplex.googleapis.com cloudaicompanion.googleapis.com >/dev/null

echo ">> Creating BigQuery datasets in ${REGION}"
for DS in "${DS_BRONZE}" "${DS_SILVER}" "${DS_GOLD}" "${DS_SEMANTIC}" \
          "${DS_AI}" "${DS_CONTROL}" "${DS_ASSERTIONS}"; do
  if bq --location="${REGION}" show --dataset "${PROJECT_ID}:${DS}" >/dev/null 2>&1; then
    echo "   - ${DS} (exists)"
  else
    bq --location="${REGION}" mk --dataset "${PROJECT_ID}:${DS}"
    echo "   - ${DS} (created)"
  fi
done

echo ">> Creating raw GCS bucket gs://${RAW_BUCKET}"
if gcloud storage buckets describe "gs://${RAW_BUCKET}" >/dev/null 2>&1; then
  echo "   - bucket exists"
else
  gcloud storage buckets create "gs://${RAW_BUCKET}" \
    --location="${REGION}" --uniform-bucket-level-access
fi

echo ">> Creating Dataform service account ${DATAFORM_SA}"
DATAFORM_SA_EMAIL="${DATAFORM_SA}@${PROJECT_ID}.iam.gserviceaccount.com"
if gcloud iam service-accounts describe "${DATAFORM_SA_EMAIL}" >/dev/null 2>&1; then
  echo "   - ${DATAFORM_SA_EMAIL} (exists)"
else
  gcloud iam service-accounts create "${DATAFORM_SA}" \
    --display-name="Airport Demo Dataform execution SA"
fi

echo ">> Granting project roles to Dataform SA"
# connectionAdmin (not just connectionUser) is required: creating resources
# WITH CONNECTION (Spark procedures, BigLake table) needs bigquery.connections.delegate.
for ROLE in roles/bigquery.dataEditor roles/bigquery.jobUser \
            roles/bigquery.connectionAdmin roles/storage.objectViewer \
            roles/dataproc.editor; do
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${DATAFORM_SA_EMAIL}" --role="${ROLE}" \
    --condition=None >/dev/null
done

echo ">> Granting Vertex AI access to the Gemini connection SA"
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${GEMINI_CONN_SA}" \
  --role="roles/aiplatform.user" --condition=None >/dev/null

echo ">> Granting BigQuery + GCS access to the Spark connection SA"
for ROLE in roles/bigquery.dataEditor roles/bigquery.jobUser \
            roles/storage.objectViewer roles/dataproc.worker; do
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SPARK_CONN_SA}" --role="${ROLE}" \
    --condition=None >/dev/null
done

echo ">> Granting the Dataform service agent token-creator on the execution SA"
# The Dataform service agent impersonates the execution SA to run workflows.
DATAFORM_AGENT="service-${PROJECT_NUMBER}@gcp-sa-dataform.iam.gserviceaccount.com"
gcloud iam service-accounts add-iam-policy-binding "${DATAFORM_SA_EMAIL}" \
  --member="serviceAccount:${DATAFORM_AGENT}" \
  --role="roles/iam.serviceAccountTokenCreator" >/dev/null

echo ">> Granting Composer worker SA the Dataform + act-as permissions"
# One describe captures both the worker SA and the DAG bucket prefix (tab-separated).
read -r COMPOSER_SA COMPOSER_DAG_GCS_PREFIX < <(gcloud composer environments describe \
  "${COMPOSER_ENV}" --location="${REGION}" \
  --format='value(config.nodeConfig.serviceAccount, config.dagGcsPrefix)')
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${COMPOSER_SA}" \
  --role="roles/dataform.admin" --condition=None >/dev/null
gcloud iam service-accounts add-iam-policy-binding "${DATAFORM_SA_EMAIL}" \
  --member="serviceAccount:${COMPOSER_SA}" \
  --role="roles/iam.serviceAccountUser" >/dev/null

echo ">> Uploading Composer DAG"
if [[ ! -f "${DAG_FILE}" ]]; then
  echo "ERROR: Composer DAG file not found: ${DAG_FILE}" >&2
  exit 1
fi
if [[ -z "${COMPOSER_DAG_GCS_PREFIX}" ]]; then
  echo "ERROR: Could not discover Composer DAG GCS prefix for ${COMPOSER_ENV}" >&2
  exit 1
fi
gcloud storage cp "${DAG_FILE}" "${COMPOSER_DAG_GCS_PREFIX}/"
echo "   - uploaded ${DAG_FILE} to ${COMPOSER_DAG_GCS_PREFIX}/"

echo ">> Bootstrap complete."
echo "   Dataform SA : ${DATAFORM_SA_EMAIL}"
echo "   Composer SA : ${COMPOSER_SA}"
echo "   Composer DAG: ${COMPOSER_DAG_GCS_PREFIX}/airport_ops_lakehouse_dag.py"
echo "   Raw bucket  : gs://${RAW_BUCKET}"
echo
echo "   NOTE: the optional data-insights script (scripts/generate_data_insights.sh)"
echo "   runs as YOUR ADC identity, which needs: roles/dataplex.dataScanEditor,"
echo "   roles/bigquery.dataViewer + roles/bigquery.dataEditor, roles/bigquery.user,"
echo "   and (for catalog publishing) roles/dataplex.catalogEditor + roles/dataplex.entryOwner."
