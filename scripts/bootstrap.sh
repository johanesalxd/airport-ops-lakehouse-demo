#!/usr/bin/env bash
#
# Idempotent provisioning for the Airport Operations Lakehouse demo.
# Creates datasets, the raw GCS bucket, a Dataform service account, grants
# the IAM required for serverless Spark stored procedures, Gemini remote models,
# Dataform execution, Pub/Sub streaming, and Composer orchestration, and uploads
# the Composer DAGs.
#
# Reuses existing BigQuery connections, Composer, and the GCP Dataform repository.
# It does NOT create those user-managed prerequisites.
#
# Usage:
#   cp .env.example .env && nano .env
#   source .env && bash scripts/bootstrap.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LAKEHOUSE_DAG_FILE="${REPO_ROOT}/composer/dags/airport_ops_lakehouse_dag.py"
STREAM_DAG_FILE="${REPO_ROOT}/composer/dags/airport_ops_baggage_stream_dag.py"
DAG_LIB_DIR="${REPO_ROOT}/composer/dags/airport_ops_lib"
SHARED_LIB_DIR="${REPO_ROOT}/airport_ops_demo"
BAGGAGE_SCHEMA_FILE="${REPO_ROOT}/schemas/baggage_scan_event.avsc"

: "${PROJECT_ID:?set PROJECT_ID (source .env)}"
: "${REGION:?set REGION}"
: "${SPARK_CONNECTION:?set SPARK_CONNECTION}"
: "${GEMINI_CONNECTION:?set GEMINI_CONNECTION}"
: "${BIGLAKE_CONNECTION:?set BIGLAKE_CONNECTION}"
: "${DATAFORM_SA:?set DATAFORM_SA}"
: "${DATAFORM_REPO_ID:?set DATAFORM_REPO_ID}"
: "${DATAFORM_GIT_BRANCH:=main}"
: "${COMPOSER_ENV:?set COMPOSER_ENV}"
: "${ADMIN_GROUP:?set ADMIN_GROUP}"
: "${SALES_GROUP:?set SALES_GROUP}"
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
: "${SPARK_CONN_SA:=}"
: "${GEMINI_CONN_SA:=}"
: "${BIGLAKE_CONN_SA:=}"
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
DATAFORM_SA_EMAIL="${DATAFORM_SA}@${PROJECT_ID}.iam.gserviceaccount.com"

connection_service_account() {
  local connection_id="$1"
  bq --format=json show --connection "${PROJECT_ID}.${REGION}.${connection_id}" \
    | python3 -c 'import json, sys; print(json.load(sys.stdin)["properties"]["serviceAccountId"])'
}

require_connection() {
  local connection_id="$1"
  local connection_type="$2"
  if ! bq show --connection "${PROJECT_ID}.${REGION}.${connection_id}" >/dev/null 2>&1; then
    echo "ERROR: Missing BigQuery ${connection_type} connection: ${REGION}.${connection_id}" >&2
    echo "Create it before running bootstrap.sh. See docs/architecture.md." >&2
    exit 1
  fi
}

set_airflow_variable() {
  local key="$1"
  local value="$2"
  gcloud composer environments run "${COMPOSER_ENV}" \
    --location="${REGION}" variables set -- "${key}" "${value}" >/dev/null
}

echo ">> Project: ${PROJECT_ID}  Region: ${REGION}"
gcloud config set project "${PROJECT_ID}" >/dev/null

echo ">> Enabling required APIs (no-op if already enabled)"
# dataplex + cloudaicompanion (Gemini for Google Cloud) power the optional
# data-insights script (scripts/generate_data_insights.sh). datalineage is
# required for Managed Service for Apache Spark lineage emitted by the Spark
# stored procedures.
gcloud services enable \
  bigquery.googleapis.com bigqueryconnection.googleapis.com \
  dataform.googleapis.com dataproc.googleapis.com \
  aiplatform.googleapis.com composer.googleapis.com \
  secretmanager.googleapis.com storage.googleapis.com \
  dataplex.googleapis.com datalineage.googleapis.com \
  cloudaicompanion.googleapis.com \
  pubsub.googleapis.com >/dev/null

echo ">> Creating Dataform service account ${DATAFORM_SA}"
if gcloud iam service-accounts describe "${DATAFORM_SA_EMAIL}" >/dev/null 2>&1; then
  echo "   - ${DATAFORM_SA_EMAIL} (exists)"
else
  gcloud iam service-accounts create "${DATAFORM_SA}" \
    --display-name="Airport Demo Dataform execution SA"
fi

echo ">> Validating user-managed prerequisites"
if ! gcloud composer environments describe "${COMPOSER_ENV}" \
    --location="${REGION}" >/dev/null 2>&1; then
  echo "ERROR: Composer environment not found: ${COMPOSER_ENV} (${REGION})" >&2
  exit 1
fi
if ! gcloud dataform repositories describe "${DATAFORM_REPO_ID}" \
    --region="${REGION}" >/dev/null 2>&1; then
  echo "ERROR: Dataform repository not found: ${DATAFORM_REPO_ID} (${REGION})" >&2
  echo "Create it manually with service account ${DATAFORM_SA_EMAIL}." >&2
  echo "Then connect it to the companion Git repo and rerun bootstrap.sh." >&2
  echo "See README.md and docs/architecture.md for the setup steps." >&2
  exit 1
fi
require_connection "${SPARK_CONNECTION}" "SPARK"
require_connection "${GEMINI_CONNECTION}" "CLOUD_RESOURCE"
require_connection "${BIGLAKE_CONNECTION}" "CLOUD_RESOURCE"

if [[ -z "${SPARK_CONN_SA}" ]]; then
  SPARK_CONN_SA="$(connection_service_account "${SPARK_CONNECTION}")"
fi
if [[ -z "${GEMINI_CONN_SA}" ]]; then
  GEMINI_CONN_SA="$(connection_service_account "${GEMINI_CONNECTION}")"
fi
if [[ -z "${BIGLAKE_CONN_SA}" ]]; then
  BIGLAKE_CONN_SA="$(connection_service_account "${BIGLAKE_CONNECTION}")"
fi

# Keep Managed Service for Apache Spark lineage enabled for Spark stored
# procedures and batch/session workloads. Runtime-level properties in the
# procedures are still the primary guarantee; this project metadata is the
# documented project-wide default and matches the live demo environment.
gcloud compute project-info add-metadata \
  --project="${PROJECT_ID}" \
  --metadata=DATAPROC_LINEAGE_ENABLED=true >/dev/null

echo ">> Creating BigQuery datasets in ${REGION}"
for DS in "${DS_BRONZE}" "${DS_SILVER}" "${DS_GOLD}" "${DS_SEMANTIC}" \
          "${DS_AI}" "${DS_CONTROL}" "${DS_ASSERTIONS}" "${DS_GOVERNANCE}"; do
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

echo ">> Creating Pub/Sub streaming baggage resources"
if [[ ! -f "${BAGGAGE_SCHEMA_FILE}" ]]; then
  echo "ERROR: Pub/Sub schema file not found: ${BAGGAGE_SCHEMA_FILE}" >&2
  exit 1
fi
if gcloud pubsub schemas describe "${PUBSUB_BAGGAGE_SCHEMA}" >/dev/null 2>&1; then
  echo "   - schema ${PUBSUB_BAGGAGE_SCHEMA} (exists)"
else
  gcloud pubsub schemas create "${PUBSUB_BAGGAGE_SCHEMA}" \
    --type=avro \
    --definition-file="${BAGGAGE_SCHEMA_FILE}" >/dev/null
  echo "   - schema ${PUBSUB_BAGGAGE_SCHEMA} (created)"
fi
if gcloud pubsub topics describe "${PUBSUB_BAGGAGE_TOPIC}" >/dev/null 2>&1; then
  echo "   - topic ${PUBSUB_BAGGAGE_TOPIC} (exists)"
else
  gcloud pubsub topics create "${PUBSUB_BAGGAGE_TOPIC}" \
    --schema="${PUBSUB_BAGGAGE_SCHEMA}" \
    --message-encoding=json >/dev/null
  echo "   - topic ${PUBSUB_BAGGAGE_TOPIC} (created)"
fi
if gcloud pubsub topics describe "${PUBSUB_BAGGAGE_DLQ_TOPIC}" >/dev/null 2>&1; then
  echo "   - topic ${PUBSUB_BAGGAGE_DLQ_TOPIC} (exists)"
else
  gcloud pubsub topics create "${PUBSUB_BAGGAGE_DLQ_TOPIC}" >/dev/null
  echo "   - topic ${PUBSUB_BAGGAGE_DLQ_TOPIC} (created)"
fi

echo ">> Creating streaming bronze table"
bq --location="${REGION}" query --use_legacy_sql=false <<SQL >/dev/null
CREATE TABLE IF NOT EXISTS \`${PROJECT_ID}.${DS_BRONZE}.brz_baggage_events_stream\` (
  event_id STRING NOT NULL,
  bag_id STRING NOT NULL,
  flight_id STRING,
  scan_type STRING,
  scan_ts TIMESTAMP,
  terminal_id STRING,
  belt_id STRING,
  status STRING,
  simulator_run_id STRING,
  subscription_name STRING,
  message_id STRING,
  publish_time TIMESTAMP,
  attributes JSON
)
PARTITION BY TIMESTAMP_TRUNC(publish_time, HOUR)
CLUSTER BY bag_id, flight_id, terminal_id
OPTIONS (
  partition_expiration_days = 3,
  description = 'Streaming bronze baggage scan events written by Pub/Sub BigQuery subscription.'
)
SQL
echo "   - ${DS_BRONZE}.brz_baggage_events_stream (ready)"

echo ">> Granting Pub/Sub service agent BigQuery write access"
PUBSUB_AGENT="service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com"
for ROLE in roles/bigquery.dataEditor roles/bigquery.metadataViewer; do
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${PUBSUB_AGENT}" --role="${ROLE}" \
    --condition=None >/dev/null
done
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${PUBSUB_AGENT}" --role="roles/pubsub.publisher" \
  --condition=None >/dev/null
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${PUBSUB_AGENT}" --role="roles/pubsub.subscriber" \
  --condition=None >/dev/null

echo ">> Creating Pub/Sub BigQuery subscription"
if gcloud pubsub subscriptions describe "${PUBSUB_BAGGAGE_SUBSCRIPTION}" >/dev/null 2>&1; then
  echo "   - subscription ${PUBSUB_BAGGAGE_SUBSCRIPTION} (exists)"
else
  gcloud pubsub subscriptions create "${PUBSUB_BAGGAGE_SUBSCRIPTION}" \
    --topic="${PUBSUB_BAGGAGE_TOPIC}" \
    --bigquery-table="${PROJECT_ID}:${DS_BRONZE}.brz_baggage_events_stream" \
    --use-topic-schema \
    --write-metadata \
    --dead-letter-topic="${PUBSUB_BAGGAGE_DLQ_TOPIC}" \
    --max-delivery-attempts=5 >/dev/null
  echo "   - subscription ${PUBSUB_BAGGAGE_SUBSCRIPTION} (created)"
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

echo ">> Granting Dataform SA the RLS/CLS policy-creation roles (governance demo)"
# Creating ROW ACCESS POLICYs needs rowAccessPolicies.create + setIamPolicy
# (-> bigquery.dataOwner); creating + granting DATA_POLICYs needs
# dataPolicies.* (-> bigquerydatapolicy.admin). dataEditor alone is insufficient.
# Granted project-wide here for demo simplicity; see roadmap (public-release prep)
# for scoping dataOwner to the governance dataset only.
for ROLE in roles/bigquery.dataOwner roles/bigquerydatapolicy.admin; do
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${DATAFORM_SA_EMAIL}" --role="${ROLE}" \
    --condition=None >/dev/null
done

echo ">> Granting the demo groups read access to RLS/CLS-protected tables"
# Members need filteredDataViewer (query RLS tables; rows auto-filtered by the
# policy grantee list) + jobUser (run queries). FINE_GRAINED_READ on the data
# policies is granted by Dataform itself (cls_staff_directory.sqlx).
for GROUP in "${ADMIN_GROUP}" "${SALES_GROUP}"; do
  for ROLE in roles/bigquery.filteredDataViewer roles/bigquery.jobUser; do
    gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
      --member="group:${GROUP}" --role="${ROLE}" --condition=None >/dev/null
  done
done

echo ">> Granting Vertex AI access to the Gemini connection SA"
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${GEMINI_CONN_SA}" \
  --role="roles/aiplatform.user" --condition=None >/dev/null

echo ">> Granting BigQuery + GCS + lineage access to the Spark connection SA"
for ROLE in roles/bigquery.dataEditor roles/bigquery.jobUser \
            roles/storage.objectViewer roles/dataproc.worker \
            roles/datalineage.producer; do
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SPARK_CONN_SA}" --role="${ROLE}" \
    --condition=None >/dev/null
done

echo ">> Granting GCS read access to the BigLake connection SA"
gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:${BIGLAKE_CONN_SA}" \
  --role="roles/storage.objectViewer" --condition=None >/dev/null

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

echo ">> Writing Airflow Variables used by the Composer DAGs"
set_airflow_variable "airport_ops_project_id" "${PROJECT_ID}"
set_airflow_variable "airport_ops_region" "${REGION}"
set_airflow_variable "airport_ops_dataform_repository_id" "${DATAFORM_REPO_ID}"
set_airflow_variable "airport_ops_dataform_git_commitish" "${DATAFORM_GIT_BRANCH}"
set_airflow_variable "airport_ops_raw_bucket" "${RAW_BUCKET}"
set_airflow_variable "airport_ops_spark_connection" "${REGION}.${SPARK_CONNECTION}"
set_airflow_variable "airport_ops_gemini_connection" "${REGION}.${GEMINI_CONNECTION}"
set_airflow_variable "airport_ops_biglake_connection" "${REGION}.${BIGLAKE_CONNECTION}"
set_airflow_variable "airport_ops_bronze_dataset" "${DS_BRONZE}"
set_airflow_variable "airport_ops_silver_dataset" "${DS_SILVER}"
set_airflow_variable "airport_ops_gold_dataset" "${DS_GOLD}"
set_airflow_variable "airport_ops_semantic_dataset" "${DS_SEMANTIC}"
set_airflow_variable "airport_ops_ai_dataset" "${DS_AI}"
set_airflow_variable "airport_ops_control_dataset" "${DS_CONTROL}"
set_airflow_variable "airport_ops_assertions_dataset" "${DS_ASSERTIONS}"
set_airflow_variable "airport_ops_governance_dataset" "${DS_GOVERNANCE}"
set_airflow_variable "airport_ops_admin_group" "${ADMIN_GROUP}"
set_airflow_variable "airport_ops_sales_group" "${SALES_GROUP}"
set_airflow_variable "airport_ops_pubsub_baggage_topic" "${PUBSUB_BAGGAGE_TOPIC}"

echo ">> Uploading Composer DAGs"
for DAG_FILE in "${LAKEHOUSE_DAG_FILE}" "${STREAM_DAG_FILE}"; do
  if [[ ! -f "${DAG_FILE}" ]]; then
    echo "ERROR: Composer DAG file not found: ${DAG_FILE}" >&2
    exit 1
  fi
done
if [[ ! -d "${DAG_LIB_DIR}" ]]; then
  echo "ERROR: Composer DAG helper module not found: ${DAG_LIB_DIR}" >&2
  exit 1
fi
if [[ ! -d "${SHARED_LIB_DIR}" ]]; then
  echo "ERROR: Shared demo module not found: ${SHARED_LIB_DIR}" >&2
  exit 1
fi
if [[ -z "${COMPOSER_DAG_GCS_PREFIX}" ]]; then
  echo "ERROR: Could not discover Composer DAG GCS prefix for ${COMPOSER_ENV}" >&2
  exit 1
fi
for DAG_FILE in "${LAKEHOUSE_DAG_FILE}" "${STREAM_DAG_FILE}"; do
  gcloud storage cp "${DAG_FILE}" "${COMPOSER_DAG_GCS_PREFIX}/"
  echo "   - uploaded ${DAG_FILE} to ${COMPOSER_DAG_GCS_PREFIX}/"
done
gcloud storage cp -r "${DAG_LIB_DIR}" "${COMPOSER_DAG_GCS_PREFIX}/"
echo "   - uploaded ${DAG_LIB_DIR} to ${COMPOSER_DAG_GCS_PREFIX}/"
gcloud storage cp -r "${SHARED_LIB_DIR}" "${COMPOSER_DAG_GCS_PREFIX}/"
echo "   - uploaded ${SHARED_LIB_DIR} to ${COMPOSER_DAG_GCS_PREFIX}/"

echo ">> Bootstrap complete."
echo "   Dataform SA : ${DATAFORM_SA_EMAIL}"
echo "   Composer SA : ${COMPOSER_SA}"
echo "   Composer DAGs: ${COMPOSER_DAG_GCS_PREFIX}/airport_ops_lakehouse_dag.py"
echo "                 ${COMPOSER_DAG_GCS_PREFIX}/airport_ops_baggage_stream_dag.py"
echo "   Raw bucket  : gs://${RAW_BUCKET}"
echo "   Stream topic: ${PUBSUB_BAGGAGE_TOPIC} -> ${DS_BRONZE}.brz_baggage_events_stream"
echo
echo "   NOTE: the optional data-insights script (scripts/generate_data_insights.sh)"
echo "   runs as YOUR ADC identity, which needs: roles/dataplex.dataScanEditor,"
echo "   roles/bigquery.dataViewer + roles/bigquery.dataEditor, roles/bigquery.user,"
echo "   and (for catalog publishing) roles/dataplex.catalogEditor + roles/dataplex.entryOwner."
