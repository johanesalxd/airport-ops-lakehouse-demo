# Demo script — Airport Operations Lakehouse (15–25 min)

A "concept then live" runbook for the workshop. Project:
`johanesa-playground-326616`, region `us-central1`.

## Assumed one-time platform setup

These exist already and are *reused* by the demo (not created by the scripts): the
three BigQuery connections (Spark, Gemini, BigLake), the Cloud Composer
environment, the GCP Dataform repository linked to the companion Git repo, and the
Secret Manager secret holding the Git token. See
[`architecture.md`](architecture.md) for how they are wired.

## 0. One-time seed (before the session)

```bash
cd airport-ops-lakehouse-demo
cp .env.example .env            # values are already correct for the demo project
source .env
bash scripts/bootstrap.sh       # datasets, bucket, SA, IAM (idempotent)
bash scripts/upload_demo_data.sh 3 42   # generate + upload 3 days of synthetic data
```

The Dataform GCP repository, GitHub connection, and Composer DAG are already
deployed. Confirm raw data landed:

```bash
gcloud storage ls gs://airport-ops-demo-605626490127/raw/
```

## 1. Set the scene (2 min) — concept

- Airport operations data is mixed-format and siloed: flights (CSV), events
  (JSON), baggage (Parquet), sensors (gzip CSV), security (nested JSON),
  feedback (multilingual JSON).
- Goal: a governed, analytics- and AI-ready lakehouse with lineage from raw file
  to KPI — and a clean foundation for a semantic layer.

## 2. Show the source files (1 min) — live

```bash
gcloud storage ls -r gs://airport-ops-demo-605626490127/raw/ | head
```

Point out the six formats and the `dt=YYYY-MM-DD` partitioning.

## 3. Architecture (3 min) — concept

Open `docs/architecture.md`. Walk the medallion flow and emphasise the layer
split:

- **Dataform** = transformation (bronze → silver → gold atomic star schema)
- **Semantic views** = roll-up at query time (swap for Looker/AtScale/Cube)
- **Spark stored procedures** = the messy ingestion, called from Dataform
- **Composer** = the outer orchestrator

## 4. Why Dataform, not Python? (2 min) — concept

Use `docs/why-dataform-not-python.md`. Show `includes/metrics.js` (logic once)
and an `assertions` block. "Declarative graph + tests + lineage for free; Spark
is called from Dataform for the non-SQL work."

## 5. Run it end-to-end from Airflow (5 min) — live

- Open the Composer **`dev-airflow`** Airflow UI → DAG `airport_ops_lakehouse`.
- Trigger it. Watch the stages: `compile_repo → setup → ingestion → bronze →
  silver → gold → semantic → quality → publish_run_summary`. The stages are the
  medallion layers — point that out as it runs.
- While it runs, open the **Dataform** page in the console → the repository →
  show the **compiled graph** (dependency DAG) and the tags. Also open
  **Workflow Execution Logs** — this is where the per-stage SQL actually executes
  and where you debug failures (the Airflow task only shows orchestration state;
  see [`operations.md`](operations.md) for the full "where logs live" guide).
- In **BigQuery → Job history**, point out a Spark procedure run and the
  `AI.GENERATE_TEXT` job.

## 6. Show the results (4 min) — live in BigQuery

Bronze metadata:

```sql
SELECT _source_format, _batch_id, COUNT(*)
FROM `johanesa-playground-326616.airport_bronze.brz_flight_schedules`
GROUP BY 1, 2;
```

Gemini multilingual enrichment:

```sql
SELECT source_language, detected_language, sentiment, urgency, topic,
       english_translation
FROM `johanesa-playground-326616.airport_silver.slv_customer_feedback_enriched`
LIMIT 15;
```

The semantic layer (this is the key moment):

```sql
-- A view. Open the definition: it is a GROUP BY over the atomic fct_flight.
SELECT * FROM `johanesa-playground-326616.airport_semantic.sem_airport_operations_daily`;
SELECT * FROM `johanesa-playground-326616.airport_semantic.sem_passenger_experience`
WHERE date_key = (SELECT MAX(date_key) FROM `johanesa-playground-326616.airport_semantic.sem_passenger_experience`);
```

Say: *"This roll-up is logical, computed on read. In production you replace this
view with Looker/AtScale/Cube — the atomic gold underneath doesn't change."*

## 7. Governance & data quality (3 min) — live

```sql
SELECT * FROM `johanesa-playground-326616.airport_gold.gold_data_quality_summary`
ORDER BY issue_count DESC;
```

The pipeline stayed green **and** caught the planted anomalies (negative counts,
orphan baggage, gate double-bookings, missing scans). Mention assertions are real
gates in the `quality` stage. Show lineage in **Dataplex / BigQuery lineage**
from raw → bronze → gold.

## 7b. (Optional) Auto-generated metadata with Gemini (3 min) — live

If you want to show the AI metadata story, run the data-insights script over the
built layers (silver + gold tables, semantic views):

```bash
source .env && bash scripts/generate_data_insights.sh --dataset-insights
```

Then in **BigQuery Studio**, select e.g. `airport_gold.fct_flight` → **Insights**
tab and show the **auto-generated table/column descriptions** and the **suggested
natural-language questions + SQL**. Select the `airport_gold` *dataset* → Insights
to show the **relationship graph** across the star schema (dims ↔ facts).

Say: *"Gemini in BigQuery profiled the data and generated documentation + starter
queries automatically — published to Knowledge Catalog for governance."* Note
this is a separate Gemini-in-BigQuery feature (dataset insights are Preview).

## 8. Close (2 min) — concept

- Transformation (Dataform) vs semantics (views/Looker) — clean separation.
- Spark for messy ingestion, called from Dataform, orchestrated by Composer.
- Extensible: RLS/CLS, Pub/Sub streaming, continuous queries, data insights
  (documented backlog in the README).

## Teardown (after)

```bash
source .env && bash scripts/teardown.sh
```
