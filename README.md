# Airport Operations Lakehouse Demo on Google Cloud

A runnable, public-safe reference demo of a **data + AI lakehouse on Google
Cloud**. It ingests six mixed-format operational data sources, transforms them
through a **medallion architecture** (bronze → silver → gold) with **Dataform**,
enriches multilingual passenger feedback with **Gemini**, models gold as an
**atomic star schema**, and exposes a **semantic view layer** — all orchestrated
end to end by **Cloud Composer**.

> **Everything is synthetic and public-safe.** No real airport, passenger, or
> proprietary data; no logos. Built for a hands-on Google Cloud data-analytics
> workshop.

---

## What this demonstrates

- **Mixed-format ingestion** with the *right tool per format*: native BigQuery
  loads, external tables (a BigLake table over columnar Parquet, and a plain
  external table over NDJSON exposing a native **`JSON` column**), and serverless
  **BigQuery Spark stored procedures** for the messy/compressed/nested files.
- **Dataform as the transformation layer** — a declarative SQL dependency graph
  with tests (assertions), documentation, and lineage, calling Spark and Gemini.
- **Gemini in BigQuery** — `AI.GENERATE_TEXT` over a remote model to translate
  and classify multilingual feedback.
- **A real Kimball star schema in gold**, kept at atomic grain, with the
  **semantic layer** as a separate query-time view layer (swappable for
  Looker / AtScale / Cube).
- **Composer (Airflow)** orchestrating the whole thing via the native Dataform
  operators, stage by stage.
- **Governance by default** — assertions as quality gates, a data-quality
  summary, and BigQuery/Dataplex lineage from raw file to KPI.

If you want the *why* behind these choices, read
[`docs/design-philosophy.md`](docs/design-philosophy.md) and
[`docs/why-dataform-not-python.md`](docs/why-dataform-not-python.md).

---

## Business storyline

> An airport operations team wants a governed, analytics- and AI-ready lakehouse.
> Flights, events, baggage, passenger-flow sensors, security queues, and
> multilingual customer feedback all arrive in different formats from different
> systems. They want clean, conformed data they can trust, AI enrichment on
> free-text feedback, and clear lineage from raw file to business KPI.

Questions the demo answers:

- Which terminals are congested, and when?
- Which flights are delayed, and what is the downstream baggage/passenger impact?
- Are baggage journeys meeting SLA?
- What are passengers complaining about *across languages*, and what's urgent?
- Can every KPI be traced back to its raw source?

---

## Architecture at a glance

```
6 synthetic sources (CSV, JSONL, Parquet, gz-CSV, nested JSON, multilingual NDJSON)
   │
   ▼  Cloud Storage raw landing  (dt=YYYY-MM-DD partitions)
   │
   ▼  Dataform operations: native loads · external tables (BigLake Parquet +
   ▼                       plain external JSON column) · Spark stored procs
   ▼  BRONZE  typed + ingestion metadata  (feedback bronze = view over external JSON)
   ▼  SILVER  conformed/cleaned + Gemini feedback enrichment
   ▼  GOLD    ATOMIC star schema (dim_* + fct_*)
   ▼  SEMANTIC  views = query-time roll-up  (→ Looker / AtScale / Cube)
   └─ assertions + data-quality summary + lineage

        Cloud Composer (Airflow) orchestrates every stage.
```

Full tech-stack and infrastructure detail (services, the two-repo design, the
GCP Dataform repository + GitHub + Secret Manager connection, reused BigQuery
connections and IAM, the Composer DAG, Spark procedures, the Gemini model) is in
[`docs/architecture.md`](docs/architecture.md).

---

## How the data flows (follow this during the live demo)

This section is the **mental model** for watching the pipeline run; the
[`docs/`](#documentation) folder has the deep-dive for anything below.

### 1. The six sources, and how each is ingested

The demo deliberately uses *the right ingestion tool per format* — that variety
is the point.

| # | Source (synthetic) | Format | Ingested by | Lands in (bronze) |
|---|---|---|---|---|
| 1 | `flight_schedules` | CSV | Native BigQuery load | `brz_flight_schedules` |
| 2 | `flight_events` | NDJSON (`.jsonl`) | Native BigQuery load | `brz_flight_events` |
| 3 | `baggage_events` | Parquet | **BigLake** external table | `brz_baggage_events` |
| 4 | `passenger_flow` | Gzip CSV | **Serverless Spark** stored proc | `brz_passenger_flow` |
| 5 | `security_wait_times` | Nested JSON | **Serverless Spark** stored proc | `brz_security_wait` |
| 6 | `customer_feedback` | NDJSON (`.jsonl`) | **Plain external table → native `JSON` column**; bronze is a **view** (see note) | `brz_customer_feedback` (view) |

> **Source 6 is a deliberate anti-pattern.** The feedback NDJSON is exposed as a
> plain external table whose whole line lands in a single native **`JSON`**
> column, and the bronze layer is a **non-materialised view** straight over it.
> It works and reads cleanly (`payload.feedback_text`), but a view over external,
> row-oriented JSON is **not performant** — every query re-scans and re-parses the
> text, unlike a materialised native table or a columnar format (Parquet). Source
> 3 (baggage) shows the *good* external-table case: columnar Parquet via BigLake.
> Gemini enrichment still happens downstream in silver.

All six land in Cloud Storage under `dt=YYYY-MM-DD/` partitions first (seeded by
`scripts/upload_demo_data.sh`).

### 2. What each orchestration task does

The Composer DAG runs these tasks **in order**. Each Dataform stage maps to a
medallion layer (or a setup step), so the task list *is* the architecture.

| Task | What it does | Key component | What to inspect |
|---|---|---|---|
| `compile_repo` | Compiles the Dataform Git repo to an execution graph; stamps `batchId = {{ run_id }}` | Dataform compilation API | Compiled graph in the Dataform UI |
| `run_setup` | Creates the Gemini remote model, the BigLake (Parquet) + plain external (JSON-column) tables, and registers the Spark stored procedures | Dataform ops + BQ connections | Model, external tables & procs exist |
| `run_ingestion` | Native loads + CALLs the 2 Spark procs to land the file-based sources | Serverless Spark + BQ loads | The `raw_*` tables populate |
| `run_bronze` | Types the raw data and adds ingestion metadata (`_source_format`, `_batch_id`); feedback bronze is a **view** projecting the external `JSON` column | Dataform / BigQuery SQL | `brz_*` schema & metadata cols |
| `run_silver` | Conforms/cleans; **Gemini** translates + classifies multilingual feedback (sentiment, urgency, topic) | `AI.GENERATE_TEXT` | `slv_customer_feedback_enriched` |
| `run_gold` | Builds the **atomic star schema** (3 dims, 3 facts); quarantines orphan records | Dataform SQL | `dim_*`, `fct_*` |
| `run_semantic` | Query-time roll-up **views** — no data materialised | BigQuery views | 3 `sem_*` views |
| `run_quality` | Runs assertions as **gates** + writes the data-quality summary | Dataform assertions | `gold_data_quality_summary`, assertion results |
| `publish_run_summary` | Smoke-tests that the semantic layer is queryable | `BigQueryInsertJob` | Query job in BQ history |

### 3. What success looks like

- **All 9 tasks green**, in order `compile_repo → … → publish_run_summary`.
- **Row counts** (3-day seed) roughly: bronze ~123 flights; gold `fct_flight`
  ~123, `fct_baggage` ~300+, `fct_feedback` ~75.
- **Semantic views return rows** — `sem_airport_operations_daily` shows a rising
  `delay_rate` and a believable `late_bag_rate` (~1/3 of bags, not all).
- **`gold_data_quality_summary` lists the planted anomalies** (missing load scans,
  extreme security waits, gate double-bookings, negative passenger counts, orphan
  baggage) — yet the run stays green, because they're *quarantined*, not fatal.
- **Where to look:** results in the `airport_bronze/silver/gold/semantic`
  datasets; to debug a failure, open **Dataform → Workflow Execution Logs** (the
  Airflow task only shows orchestration state — see
  [`docs/operations.md`](docs/operations.md)).

---

## Repository layout (two repos)

| Repo | Role |
|---|---|
| **`airport-ops-lakehouse-demo`** (this repo) | Everything *around* the transformation: data generator, provisioning scripts, the Composer DAG, docs. |
| **[`airport-ops-lakehouse-dataform`](https://github.com/johanesalxd/airport-ops-lakehouse-dataform)** | The Dataform project (the transformation graph). |

Two repos because a **GCP Dataform repository expects the Dataform project at the
Git repo root**, and that repository is what Composer invokes. See
[`docs/architecture.md`](docs/architecture.md#two-repo-design).

```
airport-ops-lakehouse-demo/
  README.md
  .env.example                      # all project/region/connection/dataset config
  scripts/
    generate_demo_data.py           # deterministic 6-source synthetic generator
    bootstrap.sh                    # datasets, bucket, SA, IAM, Composer DAG upload
    upload_demo_data.sh             # generate + upload to GCS
    teardown.sh                     # remove demo resources (keeps shared connections)
  composer/dags/
    airport_ops_lakehouse_dag.py    # the end-to-end orchestration DAG
  docs/
    architecture.md                 # tech stack + infrastructure
    design-philosophy.md            # medallion + star schema vs semantic layer
    why-dataform-not-python.md
    demo-script.md                  # the workshop runbook
    roadmap.md                      # what's next (governance, streaming, insights)
    gcp-docs.md                     # official GCP documentation map
  sample_data/                      # one day of generated output, for reference
```

---

## Prerequisites

- A GCP project with BigQuery, Dataform, Dataproc, Vertex AI, Composer, Secret
  Manager, and Cloud Storage APIs enabled (`bootstrap.sh` enables them).
- `gcloud` authenticated (`gcloud auth application-default login`) with rights to
  create datasets, buckets, service accounts, and IAM bindings.
- **Assumed already provisioned** (the demo *reuses* these rather than creating
  them): three BigQuery connections — a **Spark** connection, a **CLOUD_RESOURCE**
  connection for Gemini, and one for BigLake — plus a **Cloud Composer**
  environment, the **GCP Dataform repository** linked to the companion Git repo,
  and the **Secret Manager** secret holding the Git token. How these are wired is
  documented in [`docs/architecture.md`](docs/architecture.md).
- Python 3, `pyarrow` (for the Parquet generator), Node + the Dataform CLI (only
  needed for local Dataform compile).

---

## How to run

```bash
# 1. Configure (defaults already target the demo project)
cp .env.example .env
source .env

# 2. Provision infra/IAM and upload the Composer DAG (idempotent)
bash scripts/bootstrap.sh

# 3. Generate + upload 3 days of synthetic data to the GCS landing zone
bash scripts/upload_demo_data.sh 3 42

# 4. Run the pipeline: trigger the `airport_ops_lakehouse` DAG in Composer.
#    It compiles the Dataform repo and runs it stage by stage:
#    setup → ingestion → bronze → silver → gold → semantic → quality

# 5. (Optional) Auto-generate BigQuery data insights — AI descriptions, suggested
#    questions + SQL, and a dataset relationship graph — over the built layers:
bash scripts/generate_data_insights.sh                 # silver + gold + semantic
bash scripts/generate_data_insights.sh --dataset-insights   # + relationship graph
```

Then explore results in BigQuery (the semantic views in `airport_semantic`, the
Gemini enrichment in `airport_silver.slv_customer_feedback_enriched`, and
`airport_gold.gold_data_quality_summary`). The
[`docs/demo-script.md`](docs/demo-script.md) is a minute-by-minute runbook.

---

## What's covered vs. not

**Covered (in this MVP):**

| Area | Implemented |
|---|---|
| Mixed-format ingestion | native loads, BigLake external table (Parquet), plain external table with a native `JSON` column, 2 Spark stored procedures |
| BigQuery `JSON` type | feedback NDJSON → single `JSON` column, queried by field access (`payload.feedback_text`); non-materialised bronze view as a deliberate anti-pattern |
| Transformation | Dataform bronze → silver → gold, `includes/` DRY logic |
| AI enrichment | Gemini remote model + `AI.GENERATE_TEXT` on multilingual feedback |
| Data modelling | atomic conformed star schema (3 dims, 3 facts) |
| Semantic layer | 3 BigQuery roll-up views |
| Data quality | built-in + manual assertions, quarantine, quality summary |
| Orchestration | Composer DAG driving Dataform by stage/tag |
| Lineage | BigQuery / Dataplex lineage from raw → gold |
| Cost control | small synthetic volumes, partition/cluster, teardown script |

**Not covered (intentionally — see the roadmap):** row-/column-level security and
masking, managed data quality (Dataplex auto DQ), Pub/Sub streaming ingestion,
BigQuery continuous queries, conversational analytics / data agents, vector
search & embeddings, an Iceberg open-table-format variant, a BI dashboard, data
sharing (Analytics Hub), and Dataform CI/CD environments. These are documented as
next steps in [`docs/roadmap.md`](docs/roadmap.md). **Automated BigQuery data
insights** is available as an optional script (see *How to run* below).

> Measured against Google Cloud's
> [end-to-end data integration](https://cloud.google.com/use-cases/data-integration)
> and [analytics lakehouse](https://docs.cloud.google.com/architecture/big-data-analytics/analytics-lakehouse)
> reference patterns, this demo covers the full ingest → transform → model →
> serve → govern path. The only canonical pieces shown as roadmap (not live) are
> the **BI/visualization layer** (Looker / Looker Studio) and **conversational
> analytics** (the BigQuery data agent) on top of the semantic layer.

---

## Documentation

| Doc | What it covers |
|---|---|
| [`docs/architecture.md`](docs/architecture.md) | Tech stack, infrastructure, the two-repo design, connections, DAG, Spark, Gemini |
| [`docs/design-philosophy.md`](docs/design-philosophy.md) | Medallion architecture; why bronze/silver/gold; **why gold is a star schema, not the semantic layer** |
| [`docs/why-dataform-not-python.md`](docs/why-dataform-not-python.md) | What Dataform is and when to reach for Spark/Python instead |
| [`docs/demo-script.md`](docs/demo-script.md) | The workshop runbook |
| [`docs/operations.md`](docs/operations.md) | Runbook: **where logs live**, Composer 3 caveats, idempotency, known issues |
| [`docs/roadmap.md`](docs/roadmap.md) | Governance, streaming, continuous queries, data insights |
| [`docs/gcp-docs.md`](docs/gcp-docs.md) | Official Google Cloud documentation map |

---

## Cost & teardown

The demo is designed to be cheap: small synthetic volumes, partitioned/clustered
facts, Gemini called only on a small feedback table. **Serverless Spark and Gemini
inference do incur cost** — keep volumes small. To remove demo resources (shared
connections are left intact):

```bash
source .env && bash scripts/teardown.sh
```

---

## Safety

Synthetic data only. No real PII, secrets, credentials, or proprietary data are
committed; `.env` is git-ignored (`.env.example` carries non-secret config).
