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
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)

# --- Configuration ----------------------------------------------------------

PROJECT_ID = "johanesa-playground-326616"
REGION = "us-central1"
REPOSITORY_ID = "airport-ops-lakehouse-dataform"
GIT_COMMITISH = "main"
SEMANTIC_DATASET = "airport_semantic"

# Pipeline stages run in this order; each maps to a Dataform tag. Each stage runs
# only the actions carrying its tag (transitive dependencies disabled), so the
# medallion layers must each be their own stage — including `bronze`.
STAGES = ["setup", "ingestion", "bronze", "silver", "gold", "semantic", "quality"]

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
            "code_compilation_config": {
                # Stamp every bronze row with the Airflow run for traceability.
                # Use run_id (always defined): a schedule=None DAG triggered
                # manually on Airflow 3 has no data interval, so date macros like
                # ds_nodash are undefined.
                "vars": {"batchId": "{{ run_id }}"},
            },
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
                "query": (
                    "SELECT date_key, total_flights, delay_rate, "
                    "avg_delay_minutes, total_scheduled_passengers, late_bag_rate "
                    f"FROM `{PROJECT_ID}.{SEMANTIC_DATASET}.sem_airport_operations_daily` "
                    "ORDER BY date_key"
                ),
                "useLegacySql": False,
            }
        },
        location=REGION,
    )

    previous >> publish_summary
