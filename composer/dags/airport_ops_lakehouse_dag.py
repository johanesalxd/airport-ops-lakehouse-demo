"""Airport Operations Lakehouse — end-to-end orchestration DAG.

Cloud Composer (Airflow) is the outer orchestrator. It drives Dataform via the
native Dataform Airflow operators:

  * DataformCreateCompilationResultOperator  -> compiles the connected Git repo
  * DataformCreateWorkflowInvocationOperator -> runs a stage (filtered by tag),
    SYNCHRONOUSLY (asynchronous=False): the task waits for the invocation to
    finish and fails if it fails. No separate state sensor is needed.

Dataform owns everything inside BigQuery: it CALLs the serverless Spark stored
procedures, runs the Gemini enrichment, and builds bronze -> silver -> gold
(atomic star schema) -> semantic views, then runs assertions.

Pre-requisite: raw synthetic data must be in the GCS landing zone. Seed it once
with `scripts/upload_demo_data.sh` (or wire an ingestion task in production).
"""

from __future__ import annotations

from datetime import datetime

from airflow import models
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)

# --- Configuration ----------------------------------------------------------

PROJECT_ID = Variable.get("airport_ops_project_id")
REGION = Variable.get("airport_ops_region", default_var="us-central1")
REPOSITORY_ID = Variable.get(
    "airport_ops_dataform_repository_id",
    default_var="airport-ops-lakehouse-dataform",
)
GIT_COMMITISH = Variable.get("airport_ops_dataform_git_commitish", default_var="main")

RAW_BUCKET = Variable.get("airport_ops_raw_bucket")
SPARK_CONNECTION = Variable.get("airport_ops_spark_connection")
GEMINI_CONNECTION = Variable.get("airport_ops_gemini_connection")
BIGLAKE_CONNECTION = Variable.get("airport_ops_biglake_connection")

BRONZE_DATASET = Variable.get("airport_ops_bronze_dataset")
SILVER_DATASET = Variable.get("airport_ops_silver_dataset")
GOLD_DATASET = Variable.get("airport_ops_gold_dataset")
SEMANTIC_DATASET = Variable.get("airport_ops_semantic_dataset")
AI_DATASET = Variable.get("airport_ops_ai_dataset")
CONTROL_DATASET = Variable.get("airport_ops_control_dataset")
ASSERTIONS_DATASET = Variable.get("airport_ops_assertions_dataset")
GOVERNANCE_DATASET = Variable.get("airport_ops_governance_dataset")

ADMIN_GROUP = Variable.get("airport_ops_admin_group")
SALES_GROUP = Variable.get("airport_ops_sales_group")

# Pipeline stages run in this order; each maps to a Dataform tag. Each stage runs
# only the actions carrying its tag (transitive dependencies disabled), so the
# medallion layers must each be their own stage — including `bronze`.
STAGES = [
    "setup",
    "ingestion",
    "bronze",
    "silver",
    "gold",
    "semantic",
    "quality",
    # Governance showcase: builds the self-contained staff_directory table and
    # attaches RLS + CLS policies. Independent of the medallion flow (nothing
    # reads staff_directory), so its access policies cannot affect the pipeline.
    "security",
]

default_args = {
    "owner": "airport-demo",
    "retries": 0,
}


def _invocation_config(tag: str) -> dict:
    """Run only actions carrying `tag`. Dependencies from earlier stages already
    exist, so we don't pull transitive dependencies (keeps each stage tight)."""
    return {
        "included_tags": [tag],
        "transitive_dependencies_included": False,
    }


def _compilation_config() -> dict:
    """Builds the Dataform compilation overrides from Airflow Variables."""
    return {
        "default_database": PROJECT_ID,
        "default_location": REGION,
        "assertion_schema": ASSERTIONS_DATASET,
        "vars": {
            # Stamp every bronze row with the Airflow run for traceability. Use
            # run_id because a schedule=None DAG has no reliable data interval.
            "batchId": "{{ run_id }}",
            "rawBucket": RAW_BUCKET,
            "sparkConnection": SPARK_CONNECTION,
            "geminiConnection": GEMINI_CONNECTION,
            "biglakeConnection": BIGLAKE_CONNECTION,
            "bronzeDataset": BRONZE_DATASET,
            "silverDataset": SILVER_DATASET,
            "goldDataset": GOLD_DATASET,
            "semanticDataset": SEMANTIC_DATASET,
            "aiDataset": AI_DATASET,
            "controlDataset": CONTROL_DATASET,
            "governanceDataset": GOVERNANCE_DATASET,
            "adminGroup": f"group:{ADMIN_GROUP}",
            "salesGroup": f"group:{SALES_GROUP}",
            "adminPrincipal": f"principalSet://goog/group/{ADMIN_GROUP}",
            "salesPrincipal": f"principalSet://goog/group/{SALES_GROUP}",
            "geminiEndpoint": "gemini-2.5-flash",
        },
    }


with models.DAG(
    dag_id="airport_ops_lakehouse",
    description="End-to-end airport ops lakehouse: Composer -> Dataform -> Spark/Gemini -> star schema + semantic views.",
    schedule=None,  # triggered manually for the demo
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["airport", "lakehouse", "dataform", "demo"],
) as dag:
    compile_repo = DataformCreateCompilationResultOperator(
        task_id="compile_repo",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={
            "git_commitish": GIT_COMMITISH,
            "code_compilation_config": _compilation_config(),
        },
    )

    previous = compile_repo

    for stage in STAGES:
        # Synchronous invocation: the task blocks until the workflow invocation
        # reaches a terminal state and raises if it fails (no sensor needed).
        invoke = DataformCreateWorkflowInvocationOperator(
            task_id=f"run_{stage}",
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            asynchronous=False,
            workflow_invocation={
                "compilation_result": (
                    "{{ task_instance.xcom_pull('compile_repo')['name'] }}"
                ),
                "invocation_config": _invocation_config(stage),
            },
        )

        previous >> invoke
        previous = invoke

    publish_summary = BigQueryInsertJobOperator(
        task_id="publish_run_summary",
        project_id=PROJECT_ID,
        configuration={
            "query": {
                # SELECT * so this smoke test stays decoupled from the view's
                # column list (it just proves the semantic layer is queryable).
                "query": (
                    f"SELECT * FROM `{PROJECT_ID}.{SEMANTIC_DATASET}."
                    "sem_airport_operations_daily` ORDER BY date_key"
                ),
                "useLegacySql": False,
            }
        },
        location=REGION,
    )

    previous >> publish_summary
