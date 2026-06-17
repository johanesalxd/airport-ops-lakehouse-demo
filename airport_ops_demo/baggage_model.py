"""Shared baggage event model for batch and streaming demos."""

from __future__ import annotations

import datetime as dt
import math
import random

TERMINALS = ["T1", "T2", "T3", "T4"]
AIRLINES = ["AA", "BB", "CC", "DD", "EE"]
BAG_SCANS = ["check_in", "security", "load", "transfer", "claim"]


def format_timestamp(value: dt.datetime) -> str:
    """Formats a UTC timestamp with second precision."""
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def make_synthetic_flight_ids(
    rng: random.Random,
    service_date: dt.date,
    count: int,
) -> list[str]:
    """Creates synthetic flight IDs using the batch generator naming pattern.

    Args:
        rng: Random source.
        service_date: Flight service date.
        count: Number of flight IDs to create.

    Returns:
        Synthetic flight IDs shaped like the batch `flight_schedules` source.
    """
    return [
        f"{rng.choice(AIRLINES)}{rng.randint(100, 999)}-{service_date.isoformat()}-{i}"
        for i in range(count)
    ]


def generate_baggage_events(
    rng: random.Random,
    service_date: dt.date,
    flight_ids: list[str],
    bag_id_prefix: str = "bag",
    include_orphan: bool = True,
) -> list[dict[str, str]]:
    """Generates baggage scan journeys from flight IDs.

    The same model is used by the batch Parquet generator and the streaming
    Pub/Sub simulator. Scan gaps are minutes apart so full check-in to claim
    journeys straddle the 45-minute SLA with a realistic on-time/late mix.

    Args:
        rng: Random source.
        service_date: Baggage service date.
        flight_ids: Flight IDs to attach bags to.
        bag_id_prefix: Prefix for generated bag IDs.
        include_orphan: Whether to plant an orphan bag referencing a missing
            flight.

    Returns:
        Baggage scan event dictionaries.
    """
    rows = []
    bag_seq = 0
    for flight_id in flight_ids:
        for _ in range(rng.randint(1, 4)):
            bag_seq += 1
            bag_id = f"{bag_id_prefix}-{service_date.isoformat()}-{bag_seq}"
            scan_dt = dt.datetime(
                service_date.year,
                service_date.month,
                service_date.day,
                rng.randint(4, 21),
                rng.randint(0, 30),
            )
            for index, scan_type in enumerate(BAG_SCANS):
                if index > 0:
                    scan_dt += dt.timedelta(minutes=rng.randint(3, 18))
                if rng.random() < 0.1 and scan_type in ("load", "transfer"):
                    continue
                rows.append(
                    {
                        "bag_id": bag_id,
                        "flight_id": flight_id,
                        "scan_type": scan_type,
                        "scan_ts": format_timestamp(scan_dt),
                        "terminal_id": rng.choice(TERMINALS),
                        "belt_id": f"belt-{rng.randint(1, 12)}",
                        "status": "ok",
                    }
                )

    if include_orphan:
        rows.append(
            {
                "bag_id": f"{bag_id_prefix}-{service_date.isoformat()}-orphan",
                "flight_id": "ZZ000-nonexistent",
                "scan_type": "load",
                "scan_ts": format_timestamp(
                    dt.datetime(
                        service_date.year,
                        service_date.month,
                        service_date.day,
                        23,
                        59,
                    )
                ),
                "terminal_id": "T1",
                "belt_id": "belt-1",
                "status": "ok",
            }
        )
    return rows


def generate_streaming_baggage_events(
    rng: random.Random,
    service_date: dt.date,
    event_count: int,
    simulator_run_id: str,
) -> list[dict[str, str]]:
    """Generates enough baggage scan events for a streaming publish run.

    Args:
        rng: Random source.
        service_date: Baggage service date.
        event_count: Minimum number of events to return.
        simulator_run_id: Stream simulator run ID.

    Returns:
        Baggage scan event dictionaries with streaming `event_id` and
        `simulator_run_id` fields added.
    """
    estimated_events_per_flight = 8
    flight_count = max(40, math.ceil(event_count / estimated_events_per_flight))
    flight_ids = make_synthetic_flight_ids(rng, service_date, flight_count)
    events = generate_baggage_events(
        rng,
        service_date,
        flight_ids,
        bag_id_prefix="bag-stream",
        include_orphan=False,
    )

    while len(events) < event_count:
        extra_flight_ids = make_synthetic_flight_ids(
            rng,
            service_date,
            max(
                10, math.ceil((event_count - len(events)) / estimated_events_per_flight)
            ),
        )
        events.extend(
            generate_baggage_events(
                rng,
                service_date,
                extra_flight_ids,
                bag_id_prefix="bag-stream",
                include_orphan=False,
            )
        )

    stream_events = []
    for sequence, event in enumerate(events[:event_count]):
        stream_events.append(
            {
                **event,
                "event_id": f"bse-{simulator_run_id}-{sequence:08d}",
                "simulator_run_id": simulator_run_id,
            }
        )
    return stream_events
