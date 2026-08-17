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
   ▼  Cloud Storage raw landing  (raw/<source>/dt=YYYY-MM-DD partitions)
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

All six land in Cloud Storage under `raw/<source>/dt=YYYY-MM-DD/` partitions
first (seeded by `scripts/upload_demo_data.sh`).

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
| `run_security` | Governance showcase: builds the self-contained `staff_directory` and attaches RLS + CLS/masking policies (independent of the pipeline) | Dataform `ROW ACCESS POLICY` + `DATA_POLICY` | `airport_governance.staff_directory`, row/data policies |
| `run_share` | Data-sharing showcase: builds the curated `shr_*` authorized views in `airport_share` (publishing the listing is a separate script) | Dataform views | `airport_share.shr_*` |
| `publish_run_summary` | Smoke-tests that the semantic layer is queryable | `BigQueryInsertJob` | Query job in BQ history |

### 3. What success looks like

- **All 11 tasks green**, in order `compile_repo → … → security → share → publish_run_summary`.
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
| **[`airport-ops-lakehouse-dataform`](https://github.com/johanesalxd/airport-ops-lakehouse-dataform)** | The Dataform project (the transformation graph). Clone or fork it beside this repo. |

Two repos because a **GCP Dataform repository expects the Dataform project at the
Git repo root**, and that repository is what Composer invokes. See
[`docs/architecture.md`](docs/architecture.md#two-repo-design).

```
airport-ops-lakehouse-demo/
  README.md
  pyproject.toml                    # Python deps, managed by uv
  .env.example                      # all project/region/connection/dataset config
  scripts/
    generate_demo_data.py           # deterministic 6-source synthetic generator
    generate_data_insights.py       # Dataplex data-insights scans (impl)
    generate_data_insights.sh       # thin wrapper for the data-insights script
    bootstrap.sh                    # datasets, bucket, SA, IAM, Composer DAG upload
    upload_demo_data.sh             # generate + upload to GCS
    setup_analytics_hub.py/.sh      # publisher: publish airport_share via Analytics Hub
    subscribe_analytics_hub.py/.sh  # subscriber: link dataset + cost-isolated query
    manage_subscriptions.py/.sh     # publisher: list/revoke subscriptions (governance)
    teardown.sh                     # remove demo resources (keeps shared connections)
  airport_ops_demo/
    baggage_model.py                # shared batch + streaming baggage model
  schemas/
    baggage_scan_event.avsc         # Pub/Sub Avro schema v1
  composer/dags/
    airport_ops_lakehouse_dag.py    # the end-to-end orchestration DAG
    airport_ops_baggage_stream_dag.py
    airport_ops_lib/                # DAG helper modules
  docs/
    README.md                       # documentation index — start here
    architecture.md                 # tech stack + infrastructure
    design-philosophy.md            # medallion + star schema vs semantic layer
    why-dataform-not-python.md
    demo-script.md                  # the workshop runbook
    operations.md                   # runbook: logs, caveats, known issues
    data-sharing.md                 # Analytics Hub hub-and-spoke runbook
    roadmap.md                      # implemented showcases and next steps
    slides/                         # Marp workshop deck (+ README for building it)
```

---

## Prerequisites

- A Google Cloud project. `bootstrap.sh` enables every API it needs: BigQuery,
  BigQuery Connection, Dataform, Dataproc, Vertex AI, Composer, Secret Manager,
  Cloud Storage, Dataplex, Data Lineage, Gemini for Google Cloud, Analytics Hub
  (also required in the subscriber project for the optional data-sharing
  showcase), Pub/Sub, and Compute Engine (needed for the project-wide Dataproc
  lineage metadata flag).
- `gcloud` (with the `bq` CLI) authenticated (`gcloud auth application-default
  login`) with rights to create datasets, buckets, service accounts, and IAM
  bindings.
- **Two Google Groups must already exist** for the RLS/CLS `security` stage:
  `bq-rls-cls-dataform-admin@<domain>` and `bq-rls-cls-dataform-sales@<domain>`.
  `bootstrap.sh` grants them `bigquery.jobUser` only — deliberately *not* a read
  role, since row access comes from the `ROW ACCESS POLICY` grantee list and
  column access from `GRANT FINE_GRAINED_READ`, both issued by Dataform. It
  can't create groups, so it will fail if they don't exist. Set them as
  `ADMIN_GROUP` / `SALES_GROUP` in `.env`.
- **Assumed already provisioned** (the demo *reuses* these rather than creating
  them): three BigQuery connections — a **Spark** connection, a **CLOUD_RESOURCE**
  connection for Gemini, and one for BigLake — plus a **Cloud Composer**
  environment and the **GCP Dataform repository** linked to the companion Git repo.
  How these are wired is documented in [`docs/architecture.md`](docs/architecture.md).
- The three BigQuery connection service accounts must be copied into `.env` as
  `SPARK_CONN_SA`, `GEMINI_CONN_SA`, and `BIGLAKE_CONN_SA`. BigQuery creates
  these identities when you create each connection; retrieve them from the
  connection details pane or with `bq show --connection PROJECT_ID.REGION.CONNECTION_ID`.
- **[uv](https://docs.astral.sh/uv/)** — manages the Python version + deps;
  the scripts run under `uv run` (see `pyproject.toml`). Run
  `uv sync` once. Node + the Dataform CLI are optional (only for local Dataform
  compile). To build the slide deck, see [`docs/slides/README.md`](docs/slides/README.md).
- `bootstrap.sh` also creates the `airport_governance` dataset and grants the
  Dataform service account the RLS/CLS policy-creation roles.
- The optional data-insights script (`scripts/generate_data_insights.sh`) runs as
  **your** ADC identity and needs extra roles: `roles/dataplex.dataScanEditor`,
  `roles/bigquery.dataViewer` + `roles/bigquery.dataEditor`, `roles/bigquery.user`
  (and for catalog publishing, `roles/dataplex.catalogEditor` +
  `roles/dataplex.entryOwner`).

---

## Manual Dataform repository setup

Before the full `bootstrap.sh` run, create the GCP Dataform repository manually
and connect it to your fork or clone of
[`airport-ops-lakehouse-dataform`](https://github.com/johanesalxd/airport-ops-lakehouse-dataform):

1. Run `bootstrap.sh` once if you want it to create the `DATAFORM_SA` service
   account. It will stop if the Dataform repository is not ready yet.
2. In the Google Cloud console, create a Dataform repository with ID
   `DATAFORM_REPO_ID`, region `REGION`, and the custom execution service account.
3. Connect the repository to your remote Git repository using either Developer
   Connect or a Secret Manager secret that stores a Git token. The
   `DATAFORM_GIT_URL` and `DATAFORM_GIT_SECRET` values in `.env` are notes for
   this manual step; `bootstrap.sh` validates the Dataform repository but does
   not create the Git connection.
4. Set the default branch to `DATAFORM_GIT_BRANCH` (`main` by default).
5. Rerun `bootstrap.sh` to validate the repository, grant IAM, set Airflow
   Variables, and upload the DAGs.

Google Cloud references:

- [Create a Dataform repository](https://docs.cloud.google.com/dataform/docs/create-repository)
- [Connect a Dataform repository to Git](https://docs.cloud.google.com/dataform/docs/connect-repository)
- [Schedule Dataform runs with Managed Airflow](https://docs.cloud.google.com/dataform/docs/schedule-runs)

## BigQuery connection service accounts

Before running `bootstrap.sh`, create the three BigQuery connections listed in
`.env` and copy each connection service account into `.env`:

```bash
bq show --connection "$PROJECT_ID.$REGION.$SPARK_CONNECTION"
bq show --connection "$PROJECT_ID.$REGION.$GEMINI_CONNECTION"
bq show --connection "$PROJECT_ID.$REGION.$BIGLAKE_CONNECTION"
```

Copy the returned `serviceAccountId` values to `SPARK_CONN_SA`,
`GEMINI_CONN_SA`, and `BIGLAKE_CONN_SA`. `bootstrap.sh` validates the connection
IDs and grants IAM to the exact service accounts configured in `.env`.

Google Cloud references:

- [Create and set up a Cloud resource connection](https://docs.cloud.google.com/bigquery/docs/create-cloud-resource-connection)
- [Connect to Spark from BigQuery](https://docs.cloud.google.com/bigquery/docs/connect-to-spark)
- [Manage BigQuery connections](https://docs.cloud.google.com/bigquery/docs/working-with-connections)

---

## How to run

```bash
# 1. Configure your project, Composer environment, connections, and groups
cp .env.example .env
$EDITOR .env
source .env

# 1b. Install Python deps into a pinned uv environment (one-time)
uv sync

# 2. Provision infra/IAM and upload the Composer DAG (idempotent)
bash scripts/bootstrap.sh

# 3. Generate + upload 3 days of synthetic data to the GCS landing zone
bash scripts/upload_demo_data.sh 3 42

# 4. Run the pipeline: trigger the `airport_ops_lakehouse` DAG in Composer.
#    It compiles the Dataform repo and runs it stage by stage:
#    setup → ingestion → bronze → silver → gold → semantic → quality → security → share

# 4b. (Optional) Publish the curated share dataset via Analytics Hub
#     (hub-and-spoke) and subscribe from the spoke project. See docs/data-sharing.md
#     The SUBSCRIBER project must also have analyticshub.googleapis.com enabled and
#     the subscriber principal needs roles/analyticshub.subscriptionOwner +
#     roles/bigquery.user in that project.
bash scripts/setup_analytics_hub.sh          # publisher: exchange + listing + whitelist
bash scripts/subscribe_analytics_hub.sh      # subscriber: linked dataset + cost-isolated query
bash scripts/manage_subscriptions.sh --list  # publisher: who has subscribed (governance)

# 5. (Optional) Auto-generate BigQuery data insights — AI descriptions, suggested
#    questions + SQL, and a dataset relationship graph — over the built layers:
bash scripts/generate_data_insights.sh                 # silver + gold + semantic
bash scripts/generate_data_insights.sh --dataset-insights   # + relationship graph
```

Then explore results in BigQuery (the semantic views in `airport_semantic`, the
Gemini enrichment in `airport_silver.slv_customer_feedback_enriched`, and
`airport_gold.gold_data_quality_summary`). The
[`docs/demo-script.md`](docs/demo-script.md) is a minute-by-minute runbook.

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
| Streaming ingestion showcase | manual Composer DAG → Pub/Sub schema/topic → BigQuery subscription → bronze stream table + silver dedupe view |
| Lineage | BigQuery / Dataplex lineage from raw → gold |
| Data sharing | Analytics Hub hub-and-spoke: `shr_*` authorized views → private Data Exchange listing, subscriber whitelisting + cost-isolated linked dataset |
| Cost control | small synthetic volumes, partition/cluster, teardown script |

**Covered as a governance showcase:** row-level + column-level security and
masking (the `security` stage — a self-contained `staff_directory` table with RLS
and SQL `DATA_POLICY` masking).

**Covered as a data-sharing showcase:** Analytics Hub **hub-and-spoke** sharing
(the `share` stage + `scripts/setup_analytics_hub.py` /
`scripts/subscribe_analytics_hub.py`) — curated `shr_*` authorized views
published as a private Data Exchange listing, per-listing subscriber
whitelisting, and a cost-isolated subscriber (spoke) flow. See
[`docs/data-sharing.md`](docs/data-sharing.md).

**Not covered (intentionally — see the roadmap):** managed data quality
(Dataplex auto DQ), BigQuery continuous queries, conversational analytics / data agents, vector
search & embeddings, an Iceberg open-table-format variant, a BI dashboard, the
privacy-preserving **Data Clean Room** variant of data sharing, and Dataform
CI/CD environments. These are documented as next steps in
[`docs/roadmap.md`](docs/roadmap.md). **Automated BigQuery data insights** is
available as an optional script (see [*How to run*](#how-to-run) above).

> Measured against Google Cloud's
> [end-to-end data integration](https://cloud.google.com/use-cases/data-integration)
> and [analytics lakehouse](https://docs.cloud.google.com/architecture/big-data-analytics/analytics-lakehouse)
> reference patterns, this demo covers the full ingest → transform → model →
> serve → govern path. The only canonical pieces shown as roadmap (not live) are
> the **BI/visualization layer** (Looker / Looker Studio) and **conversational
> analytics** (the BigQuery data agent) on top of the semantic layer.

---

## Documentation

Start at the **[`docs/` index](docs/README.md)**, which maps each doc to the
question it answers. In brief:

| Doc | What it covers |
|---|---|
| [`docs/README.md`](docs/README.md) | **Documentation index — start here** |
| [`docs/architecture.md`](docs/architecture.md) | Tech stack, infrastructure, the two-repo design (+ the third path: UI-created pipelines), connections, DAG, Spark, Gemini |
| [`docs/design-philosophy.md`](docs/design-philosophy.md) | Medallion architecture; **why gold is a star schema, not the semantic layer**; **two doors to one engine** (engineer vs analyst pipelines) |
| [`docs/why-dataform-not-python.md`](docs/why-dataform-not-python.md) | What Dataform is and when to reach for Spark/Python instead |
| [`docs/demo-script.md`](docs/demo-script.md) | The workshop runbook (checklist, live flow, Q&A) |
| [`docs/operations.md`](docs/operations.md) | Runbook: **where logs live**, Composer 3 caveats, idempotency, known issues |
| [`docs/streaming-ingestion.md`](docs/streaming-ingestion.md) | Optional Pub/Sub baggage stream, schema versioning, replay/backfill |
| [`docs/data-sharing.md`](docs/data-sharing.md) | Analytics Hub hub-and-spoke: curated share views, publish/subscribe, cost isolation, subscription governance, audit |
| [`docs/roadmap.md`](docs/roadmap.md) | Governance, streaming, continuous queries, data insights, CI/CD environments |
| [`docs/slides/README.md`](docs/slides/README.md) | The Marp workshop deck and how to build/update it |

Official Google Cloud docs are linked inline where each topic is discussed.

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
committed. Sample data is regenerated locally with `scripts/generate_demo_data.py`
and uploaded by `scripts/upload_demo_data.sh`; it is not committed. `.env` is
git-ignored (`.env.example` carries placeholders and non-secret defaults).
