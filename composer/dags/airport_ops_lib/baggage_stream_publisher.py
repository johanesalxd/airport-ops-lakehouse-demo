"""Publish low-rate synthetic baggage scan events to Pub/Sub.

This module is imported by the manual streaming demo DAG at task execution time.
Keeping publisher logic outside the DAG file keeps Airflow parsing lightweight and
makes the event generator easier to test locally.
"""

from __future__ import annotations

import datetime as dt
import json
import random
import time
import uuid
from dataclasses import dataclass

from airport_ops_demo.baggage_model import generate_streaming_baggage_events
from google.cloud import pubsub_v1

MAX_EVENTS_PER_SECOND = 10
MAX_DURATION_SECONDS = 900
DEFAULT_EVENTS_PER_SECOND = 5
DEFAULT_DURATION_SECONDS = 600


@dataclass(frozen=True)
class PublishSummary:
    """Summary of a baggage stream publish run."""

    simulator_run_id: str
    events_requested: int
    events_published: int
    duplicates_published: int
    duration_seconds: int
    events_per_second: int


def _clamp(value: int | None, default: int, maximum: int) -> int:
    """Returns a positive value capped at the configured maximum."""
    if value is None:
        return default
    return max(1, min(int(value), maximum))


def publish_baggage_stream(
    project_id: str,
    topic_id: str,
    events_per_second: int | None = None,
    duration_seconds: int | None = None,
    simulator_run_id: str | None = None,
    backfill_run_id: str | None = None,
    seed: int = 42,
) -> dict[str, int | str]:
    """Publishes synthetic baggage events to a schema-backed Pub/Sub topic.

    Args:
        project_id: Google Cloud project ID.
        topic_id: Pub/Sub topic ID.
        events_per_second: Target publish rate. Capped at 10 for demo safety.
        duration_seconds: Publish duration. Capped at 900 seconds.
        simulator_run_id: Optional run ID. Defaults to a short UUID.
        backfill_run_id: Optional label used when replaying historical events.
        seed: Random seed for deterministic synthetic choices.

    Returns:
        Summary dictionary suitable for Airflow task logs/XCom.
    """
    rate = _clamp(events_per_second, DEFAULT_EVENTS_PER_SECOND, MAX_EVENTS_PER_SECOND)
    duration = _clamp(duration_seconds, DEFAULT_DURATION_SECONDS, MAX_DURATION_SECONDS)
    run_id = simulator_run_id or f"stream-{uuid.uuid4().hex[:12]}"
    rng = random.Random(seed)
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    total_events = rate * duration
    service_date = dt.datetime.now(dt.timezone.utc).date()
    events = generate_streaming_baggage_events(rng, service_date, total_events, run_id)
    published = 0
    duplicates = 0
    last_event: dict[str, str] | None = None

    attributes = {
        "event_source": "airport_ops_baggage_stream_demo",
        "schema_demo_version": "v1",
        "simulator_run_id": run_id,
    }
    if backfill_run_id:
        attributes["backfill_run_id"] = backfill_run_id

    for second in range(duration):
        started = time.monotonic()
        futures = []
        for index in range(rate):
            sequence = second * rate + index
            event = events[sequence]
            # Publish an occasional duplicate to demonstrate at-least-once dedupe.
            if sequence > 0 and sequence % 100 == 0 and last_event is not None:
                event = last_event
                duplicates += 1
            payload = json.dumps(event, separators=(",", ":")).encode("utf-8")
            futures.append(publisher.publish(topic_path, payload, **attributes))
            last_event = event

        for future in futures:
            future.result(timeout=30)
            published += 1

        sleep_seconds = 1.0 - (time.monotonic() - started)
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    summary = PublishSummary(
        simulator_run_id=run_id,
        events_requested=total_events,
        events_published=published,
        duplicates_published=duplicates,
        duration_seconds=duration,
        events_per_second=rate,
    )
    return summary.__dict__
