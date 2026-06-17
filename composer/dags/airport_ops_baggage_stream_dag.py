"""Manual Pub/Sub baggage streaming demo DAG.

This DAG is an optional showcase beside the main lakehouse build. It publishes a
low-rate stream of synthetic baggage scan events to a schema-backed Pub/Sub topic
for a bounded duration. A Pub/Sub BigQuery subscription writes those events into
the bronze streaming table.
"""

from __future__ import annotations

from datetime import datetime

from airflow import models
from airflow.operators.python import PythonOperator, get_current_context

PROJECT_ID = "johanesa-playground-326616"
PUBSUB_BAGGAGE_TOPIC = "baggage-events"

DEFAULT_ARGS = {
    "owner": "airport-demo",
    "retries": 0,
}


def _publish_stream() -> dict:
    """Publishes a bounded low-rate baggage stream from DAG run config."""
    from airport_ops_lib.baggage_stream_publisher import publish_baggage_stream

    context = get_current_context()
    conf = (context.get("dag_run") and context["dag_run"].conf) or {}
    return publish_baggage_stream(
        project_id=PROJECT_ID,
        topic_id=PUBSUB_BAGGAGE_TOPIC,
        events_per_second=conf.get("events_per_second"),
        duration_seconds=conf.get("duration_seconds"),
        simulator_run_id=conf.get("simulator_run_id") or context["run_id"],
        backfill_run_id=conf.get("backfill_run_id"),
        seed=int(conf.get("seed", 42)),
    )


with models.DAG(
    dag_id="airport_ops_baggage_stream_demo",
    description="Optional low-rate Pub/Sub baggage event simulator for the lakehouse demo.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["airport", "streaming", "pubsub", "demo"],
) as dag:
    publish_baggage_events = PythonOperator(
        task_id="publish_baggage_events",
        python_callable=_publish_stream,
    )
