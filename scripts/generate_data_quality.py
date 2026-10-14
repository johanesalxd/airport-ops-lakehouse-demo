#!/usr/bin/env python3
"""Generate Dataplex Auto Data Quality scans for the lakehouse gold layer.

Data Quality scans are managed Dataplex "DATA_QUALITY" data scans that evaluate
declarative rules against BigQuery tables, publish quality metrics directly to
Knowledge Catalog, and optionally export detailed per-row and per-rule evaluation
results to a BigQuery governance table.

For each target gold fact table (fct_flight, fct_baggage, fct_feedback), this script:
  1. (Optional) fetches profile-based recommended rules from the corresponding
     DATA_PROFILE scan (dp-fct-*), providing a non-fatal baseline preview.
  2. Ensures the DATA_QUALITY scan exists (dq-fct-*) with catalog publishing and
     BigQuery results export (airport_governance.dq_scan_results) enabled.
  3. Executes the scan and polls the job to completion.
  4. Attaches Knowledge Catalog quality publishing labels to the table.

Target tables and expected outcomes:
  - fct_flight: 9 rules (completeness, uniqueness, validity sets, referential FK
    assertion, range check) -> 100% pass score.
  - fct_baggage: 7 rules (completeness, uniqueness, range, referential integrity,
    and a planted anomaly rule `no-missing-load-scan`) -> 6 pass 100%, 1 catches
    the planted missing load scan anomaly (~91.25% pass ratio), demonstrating
    operational alert surfacing in Knowledge Catalog.
  - fct_feedback: 6 rules (completeness, uniqueness, sentiment/urgency sets,
    rating range 1..5, terminal_id set) -> 100% pass score.

Prerequisites (see scripts/bootstrap.sh):
  - APIs: dataplex.googleapis.com, bigquery.googleapis.com
  - Dataplex Service Agent (service-${PROJECT_NUMBER}@gcp-sa-dataplex.iam.gserviceaccount.com)
    needs roles/bigquery.dataViewer on scanned datasets/tables and roles/bigquery.dataEditor
    on the governance dataset to auto-create and write the dq_scan_results table.
  - Caller ADC needs: roles/dataplex.dataScanEditor, roles/bigquery.dataViewer,
    roles/bigquery.dataEditor, roles/dataplex.catalogEditor, roles/dataplex.entryOwner.

Usage:
    source .env && python3 scripts/generate_data_quality.py
    # (or via the wrapper: bash scripts/generate_data_quality.sh [--dry-run | --recommend-only])
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys

# Ensure local script directory is on sys.path to import generate_data_insights
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from generate_data_insights import (  # noqa: E402
    Dataplex,
    bq_set_labels,
    env,
    slug,
    table_resource,
)

RULE_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9-]{0,61}[A-Za-z0-9]$")


def verify_table_columns(
    project: str, dataset: str, table: str, rules: list[dict]
) -> bool:
    """Preflight check: verify all column-level rules reference columns in the table schema."""
    target = f"{project}:{dataset}.{table}"
    res = subprocess.run(
        ["bq", "show", "--format=json", target],
        capture_output=True,
        text=True,
    )
    if res.returncode != 0:
        print(
            f"   ! bq show {target} failed: {res.stderr.strip()[:160]}",
            file=sys.stderr,
        )
        return False

    try:
        table_meta = json.loads(res.stdout or "{}")
        fields = {
            f["name"]
            for f in table_meta.get("schema", {}).get("fields", [])
            if "name" in f
        }
    except Exception as e:
        print(f"   ! failed to parse schema for {target}: {e}", file=sys.stderr)
        return False

    missing = []
    for r in rules:
        col = r.get("column")
        if col and col not in fields:
            missing.append((r.get("name", "unnamed"), col))

    if missing:
        for r_name, col in missing:
            print(
                f"   ! schema validation error on {target}: rule '{r_name}' references "
                f"column '{col}' which does not exist in table schema!",
                file=sys.stderr,
            )
        return False

    return True


def generate_quality_rules(dp: Dataplex, profile_scan_id: str) -> list[dict]:
    """Fetch profile-based rule recommendations from a completed DATA_PROFILE scan."""
    code, body = dp._request(
        "POST", f"{dp.base}/dataScans/{profile_scan_id}:generateDataQualityRules", {}
    )
    if code == 200:
        return body.get("rule", [])
    print(
        f"   ! could not get recommended rules from {profile_scan_id} (HTTP {code}): "
        f"{dp._err(body)}",
        file=sys.stderr,
    )
    print(
        f"     Hint: run scripts/generate_data_insights.sh first so profile scan "
        f"{profile_scan_id} has a completed job.",
        file=sys.stderr,
    )
    return []


def build_gold_dq_rules(project: str, gold_ds: str) -> dict[str, list[dict]]:
    """Build curated gold fact table data quality rules.

    Note: Enum values (T1..T4, status, delay_bucket, sentiment, urgency) mirror
    includes/constants.js in the companion airport-ops-lakehouse-dataform repo.
    The fct_baggage -> fct_flight sqlAssertion is an illustrative cross-table
    referential check for the catalog UI (since fct_baggage.sqlx already inner-joins fct_flight).
    """
    flight_rules = [
        {
            "name": "flight-id-not-null",
            "description": "flight_id must not be null",
            "column": "flight_id",
            "dimension": "COMPLETENESS",
            "nonNullExpectation": {},
            "threshold": 1.0,
        },
        {
            "name": "flight-id-unique",
            "description": "flight_id must be unique across all flights",
            "column": "flight_id",
            "dimension": "UNIQUENESS",
            "uniquenessExpectation": {},
            "threshold": 1.0,
        },
        {
            "name": "date-key-not-null",
            "description": "date_key partition column must not be null",
            "column": "date_key",
            "dimension": "COMPLETENESS",
            "nonNullExpectation": {},
            "threshold": 1.0,
        },
        {
            "name": "terminal-id-not-null",
            "description": "terminal_id must not be null",
            "column": "terminal_id",
            "dimension": "COMPLETENESS",
            "nonNullExpectation": {},
            "threshold": 1.0,
        },
        {
            "name": "airline-code-not-null",
            "description": "airline_code must not be null",
            "column": "airline_code",
            "dimension": "COMPLETENESS",
            "nonNullExpectation": {},
            "threshold": 1.0,
        },
        {
            "name": "status-valid-set",
            "description": "status must be one of on_time, delayed, cancelled",
            "column": "status",
            "dimension": "VALIDITY",
            "setExpectation": {
                "values": ["on_time", "delayed", "cancelled"]
            },
            "threshold": 1.0,
        },
        {
            "name": "delay-bucket-valid-set",
            "description": "delay_bucket must be one of on_time, minor, major, severe, unknown",
            "column": "delay_bucket",
            "dimension": "VALIDITY",
            "setExpectation": {
                "values": ["on_time", "minor", "major", "severe", "unknown"]
            },
            "threshold": 1.0,
        },
        {
            "name": "airline-fk-integrity",
            "description": "Cross-table referential integrity check: every airline_code in fct_flight must exist in dim_airline",
            "dimension": "CONSISTENCY",
            "sqlAssertion": {
                "sqlStatement": (
                    f"SELECT flight_id FROM `{project}.{gold_ds}.fct_flight` f "
                    f"WHERE NOT EXISTS (SELECT 1 FROM `{project}.{gold_ds}.dim_airline` a "
                    f"WHERE a.airline_code = f.airline_code)"
                )
            },
        },
        {
            "name": "scheduled-passengers-non-negative",
            "description": "scheduled_passengers must be greater than or equal to 0",
            "column": "scheduled_passengers",
            "dimension": "VALIDITY",
            "rowConditionExpectation": {
                "sqlExpression": "scheduled_passengers >= 0"
            },
            "threshold": 1.0,
        },
    ]

    baggage_rules = [
        {
            "name": "bag-id-not-null",
            "description": "bag_id must not be null",
            "column": "bag_id",
            "dimension": "COMPLETENESS",
            "nonNullExpectation": {},
            "threshold": 1.0,
        },
        {
            "name": "bag-id-unique",
            "description": "bag_id must be unique across all baggage journeys",
            "column": "bag_id",
            "dimension": "UNIQUENESS",
            "uniquenessExpectation": {},
            "threshold": 1.0,
        },
        {
            "name": "flight-id-not-null",
            "description": "flight_id must not be null",
            "column": "flight_id",
            "dimension": "COMPLETENESS",
            "nonNullExpectation": {},
            "threshold": 1.0,
        },
        {
            "name": "scan-count-positive",
            "description": "scan_count must be at least 1",
            "column": "scan_count",
            "dimension": "VALIDITY",
            "rowConditionExpectation": {
                "sqlExpression": "scan_count >= 1"
            },
            "threshold": 1.0,
        },
        {
            "name": "journey-minutes-non-negative",
            "description": "journey_minutes must be non-negative",
            "column": "journey_minutes",
            "dimension": "VALIDITY",
            "rowConditionExpectation": {
                "sqlExpression": "journey_minutes >= 0"
            },
            "threshold": 1.0,
        },
        {
            "name": "flight-fk-integrity",
            "description": "Cross-table referential integrity check: every flight_id in fct_baggage must exist in fct_flight",
            "dimension": "CONSISTENCY",
            "sqlAssertion": {
                "sqlStatement": (
                    f"SELECT bag_id FROM `{project}.{gold_ds}.fct_baggage` b "
                    f"WHERE NOT EXISTS (SELECT 1 FROM `{project}.{gold_ds}.fct_flight` f "
                    f"WHERE f.flight_id = b.flight_id)"
                )
            },
        },
        {
            "name": "no-missing-load-scan",
            "description": "Planted anomaly detection: baggage journey must not have missing load scan (demonstrates operational quality alerts)",
            "column": "has_missing_load_scan",
            "dimension": "VALIDITY",
            "rowConditionExpectation": {
                "sqlExpression": "has_missing_load_scan = FALSE"
            },
            "threshold": 1.0,
        },
    ]

    feedback_rules = [
        {
            "name": "feedback-id-not-null",
            "description": "feedback_id must not be null",
            "column": "feedback_id",
            "dimension": "COMPLETENESS",
            "nonNullExpectation": {},
            "threshold": 1.0,
        },
        {
            "name": "feedback-id-unique",
            "description": "feedback_id must be unique across all customer feedbacks",
            "column": "feedback_id",
            "dimension": "UNIQUENESS",
            "uniquenessExpectation": {},
            "threshold": 1.0,
        },
        {
            "name": "sentiment-valid-set",
            "description": "sentiment must be one of positive, neutral, negative",
            "column": "sentiment",
            "dimension": "VALIDITY",
            "setExpectation": {
                "values": ["positive", "neutral", "negative"]
            },
            "threshold": 1.0,
        },
        {
            "name": "urgency-valid-set",
            "description": "urgency must be one of low, medium, high",
            "column": "urgency",
            "dimension": "VALIDITY",
            "setExpectation": {
                "values": ["low", "medium", "high"]
            },
            "threshold": 1.0,
        },
        {
            "name": "rating-range-1-to-5",
            "description": "rating must be an integer between 1 and 5",
            "column": "rating",
            "dimension": "VALIDITY",
            "rangeExpectation": {
                "minValue": "1",
                "maxValue": "5"
            },
            "threshold": 1.0,
        },
        {
            "name": "terminal-id-valid-set",
            "description": "terminal_id must be one of T1, T2, T3, T4",
            "column": "terminal_id",
            "dimension": "VALIDITY",
            "setExpectation": {
                "values": ["T1", "T2", "T3", "T4"]
            },
            "threshold": 1.0,
        },
    ]

    all_targets = {
        "fct_flight": flight_rules,
        "fct_baggage": baggage_rules,
        "fct_feedback": feedback_rules,
    }

    # Validate all rule names against Dataplex naming constraints
    for tbl, rules in all_targets.items():
        for r in rules:
            rname = r.get("name", "")
            if not RULE_NAME_RE.match(rname):
                raise ValueError(
                    f"Rule name '{rname}' in table '{tbl}' does not match "
                    f"Dataplex regex ^[A-Za-z][A-Za-z0-9-]{{0,61}}[A-Za-z0-9]$"
                )

    return all_targets


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate Dataplex Auto Data Quality scans for the lakehouse."
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate rule schemas and print payloads without creating/running scans",
    )
    mode_group.add_argument(
        "--recommend-only",
        action="store_true",
        help="Fetch profile-based rule recommendations only and exit",
    )

    export_group = parser.add_mutually_exclusive_group()
    export_group.add_argument(
        "--no-export",
        action="store_true",
        help="Disable BigQuery export of scan results",
    )
    export_group.add_argument(
        "--export-table",
        default=None,
        help="Table ID for DQ scan results export (default: dq_scan_results)",
    )

    parser.add_argument(
        "--save-recommendations",
        metavar="PATH",
        help="Path to save recommended rules JSON",
    )
    parser.add_argument(
        "--skip-schema-check",
        action="store_true",
        help="Skip bq show column preflight verification",
    )

    args = parser.parse_args()

    project = env("PROJECT_ID")
    location = env("REGION")
    gold_ds = env("DS_GOLD")
    gov_ds = env("DS_GOVERNANCE")

    export_table_name = args.export_table or os.environ.get(
        "DQ_EXPORT_TABLE", "dq_scan_results"
    )
    export_res = (
        None
        if args.no_export
        else (
            f"//bigquery.googleapis.com/projects/{project}/datasets/{gov_ds}/tables/{export_table_name}"
        )
    )

    print(f">> Dataplex Auto Data Quality for {project} ({location})")
    print(f">> Gold dataset: {gold_ds}  Governance dataset: {gov_ds}")
    if export_res:
        print(f">> BigQuery Export Table: {export_res}")
    else:
        print(">> BigQuery Export: DISABLED (--no-export)")

    rules_by_table = build_gold_dq_rules(project, gold_ds)

    on_demand = {"executionSpec": {"trigger": {"onDemand": {}}}}

    if args.dry_run:
        print(">> Mode: --dry-run (validating rules and schemas without mutating scans)")
        all_valid = True
        for table, rules in rules_by_table.items():
            scan_id = f"dq-{slug(table)}"
            res = table_resource(project, gold_ds, table)
            description = f"Airport Ops Gold: {table} Data Quality Scan"

            dq_spec: dict = {
                "rules": rules,
                "catalogPublishingEnabled": True,
            }
            if export_res:
                dq_spec["postScanActions"] = {
                    "bigqueryExport": {"resultsTable": export_res}
                }

            scan_spec = {
                "data": {"resource": res},
                "type": "DATA_QUALITY",
                "description": description,
                "dataQualitySpec": dq_spec,
                **on_demand,
            }
            labels = {
                "dataplex-dq-published-scan": scan_id,
                "dataplex-dq-published-project": project,
                "dataplex-dq-published-location": location,
            }

            print(f"   - Target table {gold_ds}.{table} ({len(rules)} rules):")
            for r in rules:
                dim = r.get("dimension", "UNKNOWN")
                col = r.get("column", "(table-level)")
                print(f"     * rule: {r['name']} [{dim}] column={col}")

            print(f"     Payload preview for {scan_id}:")
            print(
                json.dumps(
                    {"scan_id": scan_id, "spec": scan_spec, "labels": labels}, indent=2
                )
            )

            if not args.skip_schema_check:
                if not verify_table_columns(project, gold_ds, table, rules):
                    all_valid = False
            else:
                print("     (skipped schema preflight check)")

        if not all_valid:
            print(">> Schema validation failed during --dry-run!", file=sys.stderr)
            return 1
        if args.skip_schema_check:
            print(">> Dry-run completed (schema preflight skipped via --skip-schema-check).")
        else:
            print(">> Dry-run schema validation passed cleanly for all gold fact tables.")
        return 0

    # For non-dry-run modes, instantiate Dataplex (mints access token)
    dp = Dataplex(project, location)

    if args.recommend_only:
        print(">> Mode: --recommend-only (querying :generateDataQualityRules on dp-*)")
        all_recs: dict[str, list[dict]] = {}
        for table in rules_by_table.keys():
            dp.refresh_token()
            profile_scan_id = f"dp-{slug(table)}"
            print(f"   - Querying recommendations from {profile_scan_id}...")
            recs = generate_quality_rules(dp, profile_scan_id)
            all_recs[table] = recs
            print(f"     Found {len(recs)} recommended rule(s)")
            for r in recs:
                print(f"       * {r.get('name', 'unnamed')}: {r.get('dimension')} {r.get('column', '')}")

        if args.save_recommendations:
            out_file = Path(args.save_recommendations)
            out_file.parent.mkdir(parents=True, exist_ok=True)
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(all_recs, f, indent=2)
            print(f">> Saved recommendations to {out_file}")
        return 0

    # Full execution
    if not args.skip_schema_check:
        print(">> Running preflight table schema verification...")
        schema_ok = True
        for table, rules in rules_by_table.items():
            if not verify_table_columns(project, gold_ds, table, rules):
                schema_ok = False
        if not schema_ok:
            print(">> Preflight schema verification failed. Exiting.", file=sys.stderr)
            return 1

    # Optional: preview profile-based recommendations
    print(">> Checking baseline profile-based recommendations (non-fatal)...")
    all_recs: dict[str, list[dict]] = {}
    for table in rules_by_table.keys():
        dp.refresh_token()
        p_scan_id = f"dp-{slug(table)}"
        recs = generate_quality_rules(dp, p_scan_id)
        all_recs[table] = recs
        print(f"   - {table} ({p_scan_id}): {len(recs)} recommended rule(s)")

    if args.save_recommendations:
        out_file = Path(args.save_recommendations)
        out_file.parent.mkdir(parents=True, exist_ok=True)
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(all_recs, f, indent=2)
        print(f">> Saved recommendations to {out_file}")

    completed = failed = 0

    for table, rules in rules_by_table.items():
        dp.refresh_token()
        scan_id = f"dq-{slug(table)}"
        res = table_resource(project, gold_ds, table)
        description = f"Airport Ops Gold: {table} Data Quality Scan"

        dq_spec: dict = {
            "rules": rules,
            "catalogPublishingEnabled": True,
        }
        if export_res:
            dq_spec["postScanActions"] = {
                "bigqueryExport": {"resultsTable": export_res}
            }

        scan_spec = {
            "data": {"resource": res},
            "type": "DATA_QUALITY",
            "description": description,
            "dataQualitySpec": dq_spec,
            **on_demand,
        }

        print(f">> Setting up and running {scan_id} on {gold_ds}.{table} ({len(rules)} rules)...")
        if not dp.ensure_scan(
            scan_id, scan_spec, update_mask="dataQualitySpec,description"
        ):
            print(f"   ! failed to create/update scan {scan_id}", file=sys.stderr)
            failed += 1
            continue

        ok, job = dp.run_and_wait_job(scan_id, f"quality {table}")
        if not ok:
            print(f"   ! scan job failed or timed out for {scan_id}", file=sys.stderr)
            failed += 1
            continue

        dq_res = job.get("dataQualityResult", {})
        passed = bool(dq_res.get("passed", False))
        score = dq_res.get("score")
        pub_status = dq_res.get("catalogPublishingStatus", {}) or job.get(
            "catalogPublishingStatus", {}
        )
        pub_state = pub_status.get("state", "UNKNOWN")

        post_scan_res = dq_res.get("postScanActionsResult", {}) or job.get(
            "postScanActionsResult", {}
        )
        export_status = post_scan_res.get("bigqueryExportResult", {})
        export_state = export_status.get("state", "NONE") if export_res else "DISABLED"

        print(
            f"   >> {scan_id} job summary: passed={passed} score={score}% "
            f"catalogPublishing={pub_state} bqExport={export_state}"
        )

        if pub_state == "FAILED":
            print(f"   ! Catalog publishing failed for {scan_id}", file=sys.stderr)
            failed += 1
            continue

        if export_res and export_state == "FAILED":
            print(f"   ! BigQuery export failed for {scan_id}", file=sys.stderr)
            failed += 1
            continue

        # Attach publishing labels to BigQuery table
        print(f"   >> Attaching Dataplex DQ publishing labels to {gold_ds}.{table}...")
        bq_set_labels(
            f"{project}:{gold_ds}.{table}",
            {
                "dataplex-dq-published-scan": scan_id,
                "dataplex-dq-published-project": project,
                "dataplex-dq-published-location": location,
            },
        )
        completed += 1

    print(f">> Done. completed={completed} failed={failed}")
    print("   View results in BigQuery Studio -> select a table -> Data quality tab.")
    if export_res:
        print(f"   Exported evaluation rows available in: {project}:{gov_ds}.{export_table_name}")

    if failed > 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
