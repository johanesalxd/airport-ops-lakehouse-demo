#!/usr/bin/env python3
"""Deterministic synthetic data generator for the Airport Operations Lakehouse demo.

Generates 6 mixed-format source datasets that exercise every ingestion pattern:

  1. flight_schedules    -> CSV            (native BigQuery load)
  2. flight_events       -> NDJSON (.jsonl)(native BigQuery load)
  3. baggage_events      -> Parquet        (BigLake external table)
  4. passenger_flow      -> Gzip CSV       (BigQuery Spark stored procedure)
  5. security_wait_times -> Nested JSON    (BigQuery Spark stored procedure)
  6. customer_feedback   -> Multilingual NDJSON (external table w/ JSON column
                            -> non-materialised bronze view; Gemini enrichment in silver)

Everything is synthetic and public-safe. A fixed seed makes the output
deterministic so the demo is repeatable. The generator also plants a handful of
realistic anomalies so the Dataform assertions have something to catch.

Usage:
    python3 scripts/generate_demo_data.py --out-dir ./out --days 3 --seed 42
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import json
import os
import random

# --- Reference dimensions (synthetic) ---------------------------------------

TERMINALS = ["T1", "T2", "T3", "T4"]
ZONES = ["check_in", "departure_hall", "transit", "arrival_hall", "retail"]
AIRLINES = ["AA", "BB", "CC", "DD", "EE"]  # fictional 2-letter codes
DESTINATIONS = ["AAA", "BBB", "CCC", "DDD", "EEE", "FFF", "GGG"]
AIRCRAFT = ["A320", "A350", "B777", "B787", "A380"]
DELAY_REASONS = ["weather", "technical", "crew", "atc", "late_inbound", None]
BAG_SCANS = ["check_in", "security", "load", "transfer", "claim"]

# Multilingual feedback templates: (language_code, text, expected_sentiment)
FEEDBACK_SAMPLES = [
    ("en", "The security queue at {t} was extremely long and stressful.", "negative"),
    ("en", "Friendly staff at {t}, very smooth boarding experience.", "positive"),
    ("id", "Antrian imigrasi di {t} sangat lama, tolong ditambah petugas.", "negative"),
    ("ms", "Tandas di {t} bersih dan selesa, terima kasih.", "positive"),
    ("zh", "{t} 的行李提取等了很久，非常不满意。", "negative"),
    ("zh", "{t} 的免税店选择很多，购物体验很棒。", "positive"),
    ("ja", "{t} の案内表示が分かりやすくて助かりました。", "positive"),
    ("ja", "{t} で搭乗ゲートが急に変更され、混乱しました。", "negative"),
    ("ko", "{t} 보안 검색대 직원이 매우 친절했습니다.", "positive"),
    ("ta", "{t} இல் விமானம் தாமதமானது, தகவல் தெளிவாக இல்லை.", "negative"),
    ("hi", "{t} पर वाई-फाई बहुत धीमा था, कृपया सुधारें।", "negative"),
    ("fr", "Le personnel du {t} a ete tres serviable, merci beaucoup.", "positive"),
    # Deliberately ambiguous / malformed text for Gemini fallback testing:
    ("en", "{t} ??? ...", "neutral"),
]


def ts(d: dt.date, hour: int, minute: int) -> str:
    """ISO-8601 timestamp string (UTC, seconds precision)."""
    return dt.datetime(d.year, d.month, d.day, hour, minute, 0).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )


def daterange(start: dt.date, days: int):
    for i in range(days):
        yield start + dt.timedelta(days=i)


# --- Generators per source --------------------------------------------------


def gen_flight_schedules(rng: random.Random, d: dt.date):
    """One row per scheduled flight occurrence. Plants a gate double-booking."""
    rows = []
    n = 40
    for i in range(n):
        airline = rng.choice(AIRLINES)
        dep_h = rng.randint(5, 22)
        dep_m = rng.choice([0, 10, 15, 20, 30, 40, 45, 50])
        terminal = rng.choice(TERMINALS)
        gate = f"{terminal}-{rng.randint(1, 25):02d}"
        flight_id = f"{airline}{rng.randint(100, 999)}-{d.isoformat()}-{i}"
        rows.append(
            {
                "flight_id": flight_id,
                "airline_code": airline,
                "flight_number": f"{airline}{rng.randint(100, 999)}",
                "origin": "HUB",
                "destination": rng.choice(DESTINATIONS),
                "scheduled_departure": ts(d, dep_h, dep_m),
                "scheduled_arrival": ts(d, min(dep_h + rng.randint(1, 6), 23), dep_m),
                "terminal_id": terminal,
                "gate": gate,
                "aircraft_type": rng.choice(AIRCRAFT),
                "scheduled_passengers": rng.randint(80, 420),
            }
        )
    # Anomaly: gate double-booking (two flights, same gate, same departure slot)
    a = rows[0]
    rows.append(
        {
            **a,
            "flight_id": a["flight_id"] + "-DUP",
            "flight_number": a["flight_number"] + "X",
        }
    )
    return rows


def gen_flight_events(rng: random.Random, d: dt.date, schedules):
    """Operational events per flight: delays, cancellations."""
    rows = []
    for s in schedules:
        roll = rng.random()
        if roll < 0.15:
            status, delay = "delayed", rng.randint(20, 180)
        elif roll < 0.18:
            status, delay = "cancelled", 0
        else:
            status, delay = "on_time", rng.choice([0, 0, 0, 5, 10])
        sched = dt.datetime.strptime(s["scheduled_departure"], "%Y-%m-%dT%H:%M:%S")
        actual = sched + dt.timedelta(minutes=delay)
        rows.append(
            {
                "event_id": f"evt-{s['flight_id']}",
                "flight_id": s["flight_id"],
                "event_type": "departure",
                "actual_ts": actual.strftime("%Y-%m-%dT%H:%M:%S"),
                "status": status,
                "delay_minutes": delay,
                "delay_reason": rng.choice(DELAY_REASONS) if delay else None,
            }
        )
    return rows


def gen_baggage_events(rng: random.Random, d: dt.date, schedules):
    """Baggage scan journey. Plants a missing-flight-reference + late scan."""
    rows = []
    flight_ids = [s["flight_id"] for s in schedules]
    bag_seq = 0
    for fid in flight_ids:
        for _ in range(rng.randint(1, 4)):
            bag_seq += 1
            bag_id = f"bag-{d.isoformat()}-{bag_seq}"
            # Realistic baggage journey: scans are MINUTES apart (not hours), so the
            # full check_in -> claim span straddles the 45-min SLA. This yields a
            # believable mix of late/on-time bags (~1/3 late) instead of every bag
            # breaching. 4 gaps of ~3-18 min => total journey ~12-72 min.
            scan_dt = dt.datetime(
                d.year, d.month, d.day, rng.randint(4, 21), rng.randint(0, 30)
            )
            for j, scan in enumerate(BAG_SCANS):
                if j > 0:
                    scan_dt += dt.timedelta(minutes=rng.randint(3, 18))
                if rng.random() < 0.1 and scan in ("load", "transfer"):
                    continue  # missing scan -> SLA gap
                rows.append(
                    {
                        "bag_id": bag_id,
                        "flight_id": fid,
                        "scan_type": scan,
                        "scan_ts": scan_dt.strftime("%Y-%m-%dT%H:%M:%S"),
                        "terminal_id": rng.choice(TERMINALS),
                        "belt_id": f"belt-{rng.randint(1, 12)}",
                        "status": "ok",
                    }
                )
    # Anomaly: baggage event referencing a non-existent flight
    rows.append(
        {
            "bag_id": f"bag-{d.isoformat()}-orphan",
            "flight_id": "ZZ000-nonexistent",
            "scan_type": "load",
            "scan_ts": ts(d, 23, 59),
            "terminal_id": "T1",
            "belt_id": "belt-1",
            "status": "ok",
        }
    )
    return rows


def gen_passenger_flow(rng: random.Random, d: dt.date):
    """Sensor passenger counts. Plants a negative (invalid) count."""
    rows = []
    seq = 0
    for h in range(5, 23):
        for terminal in TERMINALS:
            for zone in ZONES:
                seq += 1
                rows.append(
                    {
                        "observation_id": f"pf-{d.isoformat()}-{seq}",
                        "terminal_id": terminal,
                        "zone_id": zone,
                        "observed_ts": ts(d, h, 0),
                        "passenger_count": rng.randint(20, 1200),
                        "sensor_id": f"sensor-{terminal}-{zone}",
                    }
                )
    rows[10]["passenger_count"] = -5  # invalid count anomaly
    return rows


def gen_security_wait_times(rng: random.Random, d: dt.date):
    """Nested JSON: checkpoints -> readings[]. Plants a high wait time."""
    checkpoints = []
    for terminal in TERMINALS:
        readings = []
        for h in range(5, 23):
            readings.append(
                {
                    "ts": ts(d, h, 0),
                    "wait_minutes": rng.randint(2, 25),
                    "queue_length": rng.randint(5, 200),
                    "lanes_open": rng.randint(2, 10),
                }
            )
        readings[8]["wait_minutes"] = 95  # high wait anomaly
        checkpoints.append(
            {
                "checkpoint_id": f"chk-{terminal}",
                "terminal_id": terminal,
                "readings": readings,
            }
        )
    return {"date": d.isoformat(), "checkpoints": checkpoints}


def gen_customer_feedback(rng: random.Random, d: dt.date):
    """Multilingual feedback records (written as NDJSON, one object per line)."""
    rows = []
    n = 25
    for i in range(n):
        lang, template, _sentiment = rng.choice(FEEDBACK_SAMPLES)
        terminal = rng.choice(TERMINALS)
        rows.append(
            {
                "feedback_id": f"fb-{d.isoformat()}-{i}",
                "terminal_id": terminal,
                "submitted_ts": ts(d, rng.randint(5, 22), rng.randint(0, 59)),
                "source_language": lang,
                "feedback_text": template.format(t=terminal),
                "rating": rng.randint(1, 5),
            }
        )
    return rows


# --- File writers -----------------------------------------------------------


def write_csv(path, rows):
    if not rows:
        return
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def write_csv_gz(path, rows):
    if not rows:
        return
    with gzip.open(path, "wt", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def write_jsonl(path, rows):
    with open(path, "w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


def write_json(path, obj):
    with open(path, "w") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def write_parquet(path, rows):
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path)


# --- Main -------------------------------------------------------------------


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="./out")
    ap.add_argument("--days", type=int, default=3)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument(
        "--start-date",
        default="2026-06-13",
        help="First dt partition (YYYY-MM-DD).",
    )
    args = ap.parse_args()

    start = dt.date.fromisoformat(args.start_date)
    rng = random.Random(args.seed)

    layout = {
        "flight_schedules": "csv",
        "flight_events": "jsonl",
        "baggage_events": "parquet",
        "passenger_flow": "csv.gz",
        "security_wait_times": "json",
        "customer_feedback": "jsonl",
    }

    for d in daterange(start, args.days):
        partition = f"dt={d.isoformat()}"
        for source in layout:
            os.makedirs(os.path.join(args.out_dir, source, partition), exist_ok=True)

        schedules = gen_flight_schedules(rng, d)
        events = gen_flight_events(rng, d, schedules)
        baggage = gen_baggage_events(rng, d, schedules)
        flow = gen_passenger_flow(rng, d)
        security = gen_security_wait_times(rng, d)
        feedback = gen_customer_feedback(rng, d)

        base = lambda s, ext: os.path.join(  # noqa: E731
            args.out_dir, s, f"dt={d.isoformat()}", f"{s}.{ext}"
        )

        write_csv(base("flight_schedules", "csv"), schedules)
        write_jsonl(base("flight_events", "jsonl"), events)
        write_parquet(base("baggage_events", "parquet"), baggage)
        write_csv_gz(base("passenger_flow", "csv.gz"), flow)
        write_json(base("security_wait_times", "json"), security)
        # Customer feedback is written as newline-delimited JSON (one object per
        # line). It is ingested via a plain external table with a single native
        # JSON column (format=CSV, tab delimiter, quoting disabled), then exposed
        # by a non-materialised bronze view -- a deliberate anti-pattern (a view
        # over external, row-oriented JSON is not performant vs a materialised or
        # columnar table). See docs/design-philosophy.md.
        write_jsonl(base("customer_feedback", "jsonl"), feedback)

        print(
            f"[{d.isoformat()}] schedules={len(schedules)} events={len(events)} "
            f"baggage={len(baggage)} flow={len(flow)} "
            f"security_checkpoints={len(security['checkpoints'])} "
            f"feedback={len(feedback)}"
        )

    print(f"\nDone. Output written under: {os.path.abspath(args.out_dir)}")


if __name__ == "__main__":
    main()
