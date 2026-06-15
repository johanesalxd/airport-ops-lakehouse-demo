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
  loads, a BigLake external table, and serverless **BigQuery Spark stored
  procedures** for the messy/compressed/nested files.
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
6 synthetic sources (CSV, JSONL, Parquet, gz-CSV, nested JSON, multilingual JSON)
   │
   ▼  Cloud Storage raw landing  (dt=YYYY-MM-DD partitions)
   │
   ▼  Dataform operations: native loads · BigLake ext table · Spark stored procs
   ▼  BRONZE  typed + ingestion metadata
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
    bootstrap.sh                    # datasets, bucket, SA, IAM (idempotent)
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

# 2. Provision datasets, bucket, service account, IAM (idempotent)
bash scripts/bootstrap.sh

# 3. Generate + upload 3 days of synthetic data to the GCS landing zone
bash scripts/upload_demo_data.sh 3 42

# 4. Run the pipeline: trigger the `airport_ops_lakehouse` DAG in Composer.
#    It compiles the Dataform repo and runs it stage by stage:
#    setup → ingestion → bronze → silver → gold → semantic → quality
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
| Mixed-format ingestion | native load, BigLake external table, 3 Spark stored procedures |
| Transformation | Dataform bronze → silver → gold, `includes/` DRY logic |
| AI enrichment | Gemini remote model + `AI.GENERATE_TEXT` on multilingual feedback |
| Data modelling | atomic conformed star schema (3 dims, 3 facts) |
| Semantic layer | 3 BigQuery roll-up views |
| Data quality | built-in + manual assertions, quarantine, quality summary |
| Orchestration | Composer DAG driving Dataform by stage/tag |
| Lineage | BigQuery / Dataplex lineage from raw → gold |
| Cost control | small synthetic volumes, partition/cluster, teardown script |

**Not covered (intentionally — see the roadmap):** row-/column-level security and
masking, Pub/Sub streaming ingestion, BigQuery continuous queries, automated
BigQuery data insights, and a BI dashboard. These are documented as next steps in
[`docs/roadmap.md`](docs/roadmap.md).

---

## Documentation

| Doc | What it covers |
|---|---|
| [`docs/architecture.md`](docs/architecture.md) | Tech stack, infrastructure, the two-repo design, connections, DAG, Spark, Gemini |
| [`docs/design-philosophy.md`](docs/design-philosophy.md) | Medallion architecture; why bronze/silver/gold; **why gold is a star schema, not the semantic layer** |
| [`docs/why-dataform-not-python.md`](docs/why-dataform-not-python.md) | What Dataform is and when to reach for Spark/Python instead |
| [`docs/demo-script.md`](docs/demo-script.md) | The workshop runbook |
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
