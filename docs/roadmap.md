# Roadmap

The MVP deliberately stops at a clean, governed batch lakehouse. These are the
natural next steps, kept out of the critical path so the core demo stays simple.
Each is grounded in current Google Cloud capabilities.

## 1. Governance: row-/column-level security & masking

Add fine-grained access control as a second-phase governance story (synthetic
data only — no real PII):

- **Row-level security (RLS):** restrict terminal-level rows by role — e.g.
  Terminal 1 operators only see `terminal_id = 'T1'` in
  `sem_terminal_performance_hourly`.
- **Column-level security (CLS):** apply policy tags to sensitive synthetic fields
  (free-text feedback, contact-like fields, operational notes).
- **Dynamic data masking:** mask those fields for lower-privilege demo users.
- **Authorized views:** expose selected gold/semantic objects to BI users while
  preserving RLS/CLS underneath.

Implementation note: BigQuery column-level access uses policy tags, and `CREATE
TABLE` DDL can't assign them directly — so policy-tag assignment needs Terraform,
`bq` schema updates, or the API rather than pure Dataform SQLX.

## 2. Streaming ingestion: baggage events via Pub/Sub

Turn baggage scans from daily Parquet files into live events:

```
baggage scanner simulator
  → Pub/Sub topic: baggage-events
  → Pub/Sub BigQuery subscription
  → airport_streaming.baggage_events_stream
  → Dataform silver model / continuous query
  → near-real-time baggage SLA + disruption tables
```

Start with a **Pub/Sub BigQuery subscription** (writes directly to BigQuery via
the Storage Write API, no subscriber client). Notes:

- BigQuery subscriptions are **at-least-once** — dedupe downstream by event/scan
  id.
- Configure a dead-letter topic for schema/write failures.
- For exactly-once or heavy windowed transforms, add a Dataflow Pub/Sub→BigQuery
  pipeline later.
- For CDC-style updates, Pub/Sub BigQuery subscriptions can drive BigQuery CDC
  with `_CHANGE_TYPE` / `_CHANGE_SEQUENCE_NUMBER`.

## 3. Real-time analytics: BigQuery continuous queries

Once events stream in:

1. **Real-time baggage SLA** — a continuous query over
   `APPENDS(TABLE airport_streaming.baggage_events_stream, …)` writing
   `airport_realtime.baggage_sla_realtime`, flagging missing/late scans.
2. **Operational alerts** — a continuous query filtering severe exceptions and
   `EXPORT DATA` to a Pub/Sub topic for downstream alerting.
3. **Real-time Gemini enrichment** — optional; call `AI.GENERATE_TEXT` over new
   rows. Keep optional: continuous AI calls get expensive.

Caveats: continuous queries are long-running jobs — document start/stop and cost
controls; verify the launch stage of stateful operations before relying on them.

## 4. Automated metadata: BigQuery data insights

A second Composer DAG that runs after the lakehouse build to auto-generate
metadata with Gemini in BigQuery:

```
wait_for_lakehouse_run
  → run Dataplex DATA_DOCUMENTATION scans on gold tables
  → generate dataset insights for airport_gold
  → publish descriptions to Knowledge Catalog (optional)
  → publish insights summary
```

Generates table/column descriptions, suggested questions + SQL, and dataset
relationship graphs. Caveats: dataset insights are Preview; data insights are
Gemini-in-BigQuery features with their own pricing; external/BigLake tables need
the scan SA to have GCS read on the underlying bucket.

## 5. BI dashboard

Point Looker (or Looker Studio) at the `airport_semantic` views — the demo's
"swap the mini semantic layer for the real one" payoff (see
[`design-philosophy.md`](design-philosophy.md)).

---

Official documentation for all of the above is mapped in
[`gcp-docs.md`](gcp-docs.md).
