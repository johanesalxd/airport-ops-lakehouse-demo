#!/usr/bin/env python3
"""Generate BigQuery data insights (Gemini in BigQuery) over the built lakehouse.

Data insights are NOT a SQL/Dataform asset: they are produced by Dataplex
"DATA_DOCUMENTATION" data scans. For each target this script:

  1. (tables only) creates + runs a DATA_PROFILE scan and publishes it via table
     labels -> grounds the insights in real values (the docs' recommendation).
  2. creates + runs a DATA_DOCUMENTATION scan (generationScopes=ALL,
     catalogPublishingEnabled=true) -> AI table/column descriptions, suggested
     natural-language questions + SQL, published to Knowledge Catalog.
  3. (tables only) attaches the data-documentation publish labels.

With --dataset-insights it also runs a dataset-level DATA_DOCUMENTATION scan per
layer -> the (Preview) relationship graph + cross-table queries.

Targets (default): silver + gold tables (profile + document) and semantic views
(document only -- profiling runs on tables, not views).

This is stdlib-only: the access token comes from `gcloud auth print-access-token`,
HTTP uses urllib, and table labels are set with `bq`. The flow handles the async
create operation (LRO) and polls the scan job to completion, and is idempotent
(an already-existing scan is reused, then re-run).

Prerequisites (see scripts/bootstrap.sh):
  - APIs: dataplex.googleapis.com, bigquery.googleapis.com,
    cloudaicompanion.googleapis.com (Gemini for Google Cloud); Gemini in BigQuery
    set up for the project.
  - The caller (your ADC identity) needs: roles/dataplex.dataScanEditor,
    roles/bigquery.dataViewer + roles/bigquery.dataEditor (on targets),
    roles/bigquery.user, and for catalog publishing roles/dataplex.catalogEditor
    + roles/dataplex.entryOwner.

Caveats (per the docs): Gemini in BigQuery does not carry the same compliance
offerings as BigQuery (fine for this synthetic demo); dataset insights are
Preview; regenerating overwrites previous insights; GEO/JSON columns and >350
columns/table aren't supported; a run does not always emit queries -- re-run to
retry.

Usage:
    source .env && python3 scripts/generate_data_insights.py [--dataset-insights]
    # (or via the wrapper: bash scripts/generate_data_insights.sh [--dataset-insights])
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

API_ROOT = "https://dataplex.googleapis.com/v1"
JOB_POLL_BUDGET_S = 1800  # max seconds to wait for a single scan job
LRO_POLL_BUDGET_S = 120  # max seconds to wait for a create operation
POLL_INTERVAL_S = 15


def env(name: str, required: bool = True, default: str | None = None) -> str:
    val = os.environ.get(name, default)
    if required and not val:
        sys.exit(f"error: {name} is not set (did you `source .env`?)")
    return val or ""


def access_token() -> str:
    return subprocess.run(
        ["gcloud", "auth", "print-access-token"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


class Dataplex:
    def __init__(self, project: str, location: str):
        self.project = project
        self.location = location
        self.base = f"{API_ROOT}/projects/{project}/locations/{location}"
        self._token = access_token()

    def _request(
        self, method: str, url: str, body: dict | None = None
    ) -> tuple[int, dict]:
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url=url, data=data, method=method)
        req.add_header("Authorization", f"Bearer {self._token}")
        req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req) as resp:
                payload = resp.read().decode() or "{}"
                return resp.status, json.loads(payload)
        except urllib.error.HTTPError as e:
            payload = e.read().decode() or "{}"
            try:
                parsed = json.loads(payload)
            except json.JSONDecodeError:
                parsed = {"error": {"message": payload}}
            return e.code, parsed
        except urllib.error.URLError as e:
            return 0, {"error": {"message": str(e)}}

    def refresh_token(self) -> None:
        self._token = access_token()

    @staticmethod
    def _err(body: dict) -> str:
        return body.get("error", {}).get("message", "") or json.dumps(body)[:200]

    def ensure_scan(self, scan_id: str, spec: dict) -> bool:
        """Create the scan if absent; reuse on ALREADY_EXISTS. Polls the LRO."""
        code, body = self._request(
            "POST", f"{self.base}/dataScans?dataScanId={scan_id}", spec
        )
        if code == 409:
            return True  # already exists -> reuse
        if code != 200:
            print(
                f"      ! create {scan_id} failed (HTTP {code}): {self._err(body)}",
                file=sys.stderr,
            )
            return False
        # create returns a long-running operation; wait until it is done.
        op_name = body.get("name", "")
        if not op_name or body.get("done"):
            return True
        waited = 0
        while waited < LRO_POLL_BUDGET_S:
            code, op = self._request("GET", f"{API_ROOT}/{op_name}")
            if code == 200 and op.get("done"):
                if "error" in op:
                    print(
                        f"      ! create {scan_id} op error: {self._err(op)}",
                        file=sys.stderr,
                    )
                    return False
                return True
            time.sleep(POLL_INTERVAL_S)
            waited += POLL_INTERVAL_S
        # Even if the op poll timed out, the scan usually exists; proceed.
        return True

    def run_and_wait(self, scan_id: str, label: str) -> bool:
        code, body = self._request("POST", f"{self.base}/dataScans/{scan_id}:run")
        if code != 200:
            print(
                f"      ! run {label} failed (HTTP {code}): {self._err(body)}",
                file=sys.stderr,
            )
            return False
        job_name = body.get("job", {}).get("name", "")
        if not job_name:
            print(f"      ! {label}: no job id returned", file=sys.stderr)
            return False
        job_id = job_name.rsplit("/", 1)[-1]
        waited = 0
        while waited < JOB_POLL_BUDGET_S:
            code, job = self._request(
                "GET", f"{self.base}/dataScans/{scan_id}/jobs/{job_id}?view=FULL"
            )
            state = job.get("state", "") if code == 200 else ""
            if state == "SUCCEEDED":
                print(f"      ok {label} (job {job_id})")
                return True
            if state in ("FAILED", "CANCELLED"):
                print(f"      ! {label} {state} (job {job_id})", file=sys.stderr)
                return False
            time.sleep(POLL_INTERVAL_S)
            waited += POLL_INTERVAL_S
        print(f"      ! {label} timed out after {waited}s", file=sys.stderr)
        return False


def slug(name: str) -> str:
    out = name.lower().replace("_", "-").replace(".", "-")
    return out[:50]


def bq_set_labels(obj: str, labels: dict[str, str]) -> None:
    args = ["bq", "update"]
    for k, v in labels.items():
        args += ["--set_label", f"{k}:{v}"]
    args.append(obj)
    res = subprocess.run(args, capture_output=True, text=True)
    if res.returncode != 0:
        print(
            f"      ! could not set labels on {obj} (non-fatal): "
            f"{res.stderr.strip()[:160]}",
            file=sys.stderr,
        )


def list_objects(project: str, dataset: str) -> list[tuple[str, str]]:
    """Return [(type, table_id), ...] for a dataset via `bq ls`."""
    res = subprocess.run(
        ["bq", "ls", "--format=json", "--max_results=1000", f"{project}:{dataset}"],
        capture_output=True,
        text=True,
    )
    if res.returncode != 0:
        print(
            f"   ! bq ls {dataset} failed: {res.stderr.strip()[:160]}", file=sys.stderr
        )
        return []
    rows = []
    for o in json.loads(res.stdout or "[]"):
        tid = o.get("tableReference", {}).get("tableId", "")
        if tid:
            rows.append((o.get("type", ""), tid))
    return rows


def table_resource(project: str, dataset: str, table: str) -> str:
    return (
        f"//bigquery.googleapis.com/projects/{project}"
        f"/datasets/{dataset}/tables/{table}"
    )


def dataset_resource(project: str, dataset: str) -> str:
    return f"//bigquery.googleapis.com/projects/{project}/datasets/{dataset}"


def main() -> int:
    ap = argparse.ArgumentParser(description="Generate BigQuery data insights.")
    ap.add_argument(
        "--dataset-insights",
        action="store_true",
        help="also generate dataset-level insights (relationship graph, Preview)",
    )
    ap.add_argument(
        "--scope",
        default=os.environ.get("INSIGHTS_SCOPE", "ALL"),
        choices=["ALL", "TABLE_AND_COLUMN_DESCRIPTIONS", "SQL_QUERIES"],
    )
    args = ap.parse_args()

    project = env("PROJECT_ID")
    location = env("REGION")
    datasets = [env("DS_SILVER"), env("DS_GOLD"), env("DS_SEMANTIC")]

    dp = Dataplex(project, location)
    on_demand = {"executionSpec": {"trigger": {"onDemand": {}}}}

    profiled = documented = failed = 0

    print(f">> Data insights for {project} ({location})")
    print(
        f">> Layers: {' '.join(datasets)}  scope={args.scope}  "
        f"dataset-insights={args.dataset_insights}"
    )

    for ds in datasets:
        print(f">> Dataset: {ds}")
        dp.refresh_token()
        objects = list_objects(project, ds)
        if not objects:
            print("   (no objects found)")

        for otype, name in objects:
            dp.refresh_token()
            is_table = otype in ("TABLE", "MATERIALIZED_VIEW")
            res = table_resource(project, ds, name)

            if is_table:
                print(f"   - table {name}: profile + document")
                # 1. profile (grounding)
                pid = f"dp-{slug(name)}"
                pspec = {
                    "data": {"resource": res},
                    "type": "DATA_PROFILE",
                    "dataProfileSpec": {},
                    **on_demand,
                }
                if dp.ensure_scan(pid, pspec) and dp.run_and_wait(
                    pid, f"profile {name}"
                ):
                    bq_set_labels(
                        f"{project}:{ds}.{name}",
                        {
                            "dataplex-dp-published-scan": pid,
                            "dataplex-dp-published-project": project,
                            "dataplex-dp-published-location": location,
                        },
                    )
                    profiled += 1
                else:
                    failed += 1
            else:
                print(f"   - view {name}: document only")

            # 2. document (table or view)
            did = f"doc-{slug(name)}"
            dspec = {
                "data": {"resource": res},
                "type": "DATA_DOCUMENTATION",
                "dataDocumentationSpec": {
                    "generationScopes": args.scope,
                    "catalogPublishingEnabled": True,
                },
                **on_demand,
            }
            if dp.ensure_scan(did, dspec) and dp.run_and_wait(did, f"document {name}"):
                if is_table:
                    bq_set_labels(
                        f"{project}:{ds}.{name}",
                        {
                            "dataplex-data-documentation-published-scan": did,
                            "dataplex-data-documentation-published-project": project,
                            "dataplex-data-documentation-published-location": location,
                        },
                    )
                documented += 1
            else:
                failed += 1

        if args.dataset_insights:
            print(f"   - dataset insights (relationship graph, Preview): {ds}")
            dp.refresh_token()
            dsid = f"doc-ds-{slug(ds)}"
            dsspec = {
                "data": {"resource": dataset_resource(project, ds)},
                "type": "DATA_DOCUMENTATION",
                "dataDocumentationSpec": {"catalogPublishingEnabled": True},
                **on_demand,
            }
            if dp.ensure_scan(dsid, dsspec) and dp.run_and_wait(dsid, f"dataset {ds}"):
                documented += 1
            else:
                failed += 1

    print(f">> Done. profiled={profiled} documented={documented} failures={failed}")
    print(
        "   View results in BigQuery Studio -> select a table/dataset -> Insights tab."
    )
    if failed:
        print(
            f">> Completed with {failed} failure(s) (see log above).", file=sys.stderr
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
