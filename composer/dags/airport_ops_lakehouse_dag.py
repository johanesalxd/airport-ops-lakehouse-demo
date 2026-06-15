"""Airport Operations Lakehouse — end-to-end orchestration DAG.

Cloud Composer (Airflow) is the outer orchestrator. It drives Dataform via the
native Dataform Airflow operators:

  * DataformCreateCompilationResultOperator  -> compiles the connected Git repo
  * DataformCreateWorkflowInvocationOperator -> runs a stage (filtered by tag)
  * DataformWorkflowInvocationStateSensor    -> waits for completion

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
from airflow.providers.google.cloud.sensors.dataform import (
    DataformWorkflowInvocationStateSensor,
)
from google.cloud.dataform_v1 import WorkflowInvocation

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
                "vars": {"batchId": "{{ ds_nodash }}-{{ run_id }}"},
            },
        },
    )

    previous = compile_repo

    for stage in STAGES:
        invoke = DataformCreateWorkflowInvocationOperator(
            task_id=f"invoke_{stage}",
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            asynchronous=True,
            workflow_invocation={
                "compilation_result": (
                    "{{ task_instance.xcom_pull('compile_repo')['name'] }}"
                ),
                "invocation_config": _invocation_config(stage),
            },
        )

        wait = DataformWorkflowInvocationStateSensor(
            task_id=f"wait_{stage}",
            project_id=PROJECT_ID,
            region=REGION,
            repository_id=REPOSITORY_ID,
            workflow_invocation_id=(
                "{{ task_instance.xcom_pull('invoke_%s')['name'].split('/')[-1] }}"
                % stage
            ),
            expected_statuses={WorkflowInvocation.State.SUCCEEDED},
            failure_statuses={
                WorkflowInvocation.State.FAILED,
                WorkflowInvocation.State.CANCELLED,
            },
            timeout=60 * 30,
            poke_interval=30,
        )

        previous >> invoke >> wait
        previous = wait

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
