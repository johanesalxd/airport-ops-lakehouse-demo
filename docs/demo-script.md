# Demo script — Airport Operations Lakehouse (15–25 min)

A "concept then live" runbook for the workshop. Use the project and region from
your `.env` file.

## Assumed one-time platform setup

These exist already and are *reused* by the demo (not created by the scripts): the
three BigQuery connections (Spark, Gemini, BigLake), the Cloud Composer
environment, the GCP Dataform repository linked to the companion Git repo, the
Secret Manager secret holding the Git token, and the **two Google Groups** for the
RLS/CLS stage (`bq-rls-cls-dataform-admin@…`, `bq-rls-cls-dataform-sales@…` —
`bootstrap.sh` grants them read roles but cannot create groups). See
[`architecture.md`](architecture.md) for how they are wired.

## Pre-flight checklist (10 min before the session)

- **Two browser profiles / windows signed in to BigQuery:**
  - **A — admin:** your normal identity, a member of `bq-rls-cls-dataform-admin@…`
    (sees all rows + raw values in §7c).
  - **B — sales:** a member of `bq-rls-cls-dataform-sales@…` (sees filtered rows +
    masked columns in §7c). Needed for the RLS/CLS reveal.
- **Tabs to pre-open:** Composer Airflow UI (`dev-airflow` → DAG
  `airport_ops_lakehouse`), BigQuery Studio, the BigQuery **Dataform** repo page,
  and the slide deck (PDF).
- **Project context:** console / `bq` set to your `PROJECT_ID`. Paste the §6–§7c
  queries into a scratch BigQuery tab ahead of time.
- **Confirm the latest run is green:** open the most recent
  `airport_ops_lakehouse` run grid — all 10 tasks green. You will **reuse** this
  run (not re-trigger) so the built tables are already populated.
- **Sanity check** one query returns rows (e.g. `sem_airport_operations_daily`).

## 0. One-time seed (before the session)

```bash
cd airport-ops-lakehouse-demo
cp .env.example .env            # fill in your project, connections, and groups
source .env
bash scripts/bootstrap.sh       # datasets, bucket, SA, IAM, Composer DAG upload
bash scripts/upload_demo_data.sh 3 42   # generate + upload 3 days of synthetic data
```

The Dataform GCP repository and GitHub connection are already deployed;
`bootstrap.sh` uploads the Composer DAG. Confirm raw data landed:

```bash
gcloud storage ls "gs://${RAW_BUCKET}/raw/"
```

## 1. Set the scene (2 min) — concept

- Airport operations data is mixed-format and siloed: flights (CSV), events
  (JSON), baggage (Parquet), sensors (gzip CSV), security (nested JSON),
  feedback (multilingual JSON).
- Goal: a governed, analytics- and AI-ready lakehouse with lineage from raw file
  to KPI — and a clean foundation for a semantic layer.

## 2. Show the source files (1 min) — live

```bash
gcloud storage ls -r "gs://${RAW_BUCKET}/raw/" | head
```

Point out the six formats and the `dt=YYYY-MM-DD` partitioning.

## 3. Architecture (3 min) — concept

Open `docs/architecture.md`. Walk the medallion flow and emphasise the layer
split:

- **Dataform** = transformation (bronze → silver → gold atomic star schema)
- **Semantic views** = roll-up at query time (swap for Looker/AtScale/Cube)
- **Spark stored procedures** = the messy ingestion (gzip CSV, nested JSON), called from Dataform
- **External tables** = baggage Parquet via BigLake (good, columnar) vs feedback NDJSON as a single `JSON` column behind a non-materialised view (the "you can, but shouldn't" anti-pattern)
- **Composer** = the outer orchestrator

## 4. Why Dataform, not Python? (2 min) — concept

Use `docs/why-dataform-not-python.md`. Show `includes/metrics.js` (logic once)
and an `assertions` block. "Declarative graph + tests + lineage for free; Spark
is called from Dataform for the non-SQL work."

## 5. Run it end-to-end from Airflow (5 min) — live

- Open the Composer **`dev-airflow`** Airflow UI → DAG `airport_ops_lakehouse`.
- **Reuse the latest green run** (recommended for the live session): open its grid
  and walk the stages `compile_repo → setup → ingestion → bronze → silver → gold →
  semantic → quality → security → publish_run_summary`. Only **trigger** a fresh
  run if you specifically want to show it execute (~8 min end-to-end). The
  medallion layers are stages — point that out.
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
FROM `your-project-id.airport_bronze.brz_flight_schedules`
GROUP BY 1, 2;
```

The BigQuery `JSON` type + the external-table anti-pattern (feedback):

```sql
-- Landing: a PLAIN EXTERNAL table over NDJSON, each line in ONE native JSON column.
SELECT payload FROM `your-project-id.airport_ops_control.raw_customer_feedback` LIMIT 3;

-- Bronze is a VIEW over it: project the JSON column by field access, no PARSE_JSON.
SELECT feedback_id, JSON_VALUE(payload.source_language) AS lang,
       payload.feedback_text AS text_json, INT64(payload.rating) AS rating
FROM `your-project-id.airport_bronze.brz_customer_feedback` LIMIT 5;
```

Say: *"The native `JSON` type lets us query `payload.feedback_text` directly. But
this bronze is a non-materialised view straight over external, row-oriented JSON —
every query re-scans and re-parses the text. It works, but it's not how you'd
serve at scale; contrast it with the columnar Parquet baggage table. Just because
you can, doesn't mean you should — you'd materialise this as a native table."*

Gemini multilingual enrichment (still happens in silver, regardless of ingestion):

```sql
SELECT source_language, detected_language, sentiment, urgency, topic,
       english_translation
FROM `your-project-id.airport_silver.slv_customer_feedback_enriched`
LIMIT 15;
```

The semantic layer (this is the key moment):

```sql
-- A view. Open the definition: it is a GROUP BY over the atomic fct_flight.
SELECT * FROM `your-project-id.airport_semantic.sem_airport_operations_daily`;
SELECT * FROM `your-project-id.airport_semantic.sem_passenger_experience`
WHERE date_key = (SELECT MAX(date_key) FROM `your-project-id.airport_semantic.sem_passenger_experience`);
```

Say: *"This roll-up is logical, computed on read. In production you replace this
view with Looker/AtScale/Cube — the atomic gold underneath doesn't change."*

## 7. Governance & data quality (3 min) — live

```sql
SELECT * FROM `your-project-id.airport_gold.gold_data_quality_summary`
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

## 7c. (Optional) Fine-grained access: RLS + CLS (3 min) — live

The `security` stage built a self-contained `airport_governance.staff_directory`
table (synthetic — nothing in the pipeline reads it) with **row-level security**
and **column masking** attached entirely from Dataform SQLX.

First show the policies exist:

```bash
bq ls --row_access_policies \
  your-project-id:airport_governance.staff_directory
```

Then have two people (or impersonate the two groups) run the **same query** and
compare:

```sql
-- bq-rls-cls-dataform-admin@  -> 6 rows, real ssn/email/salary, bank_account visible
-- bq-rls-cls-dataform-sales@  -> 2 rows (Sales only), email "" / salary NULL /
--                                ssn SHA256, and bank_account ERRORS (blocked)
SELECT staff_id, name, email, department, salary, ssn
FROM `your-project-id.airport_governance.staff_directory`
ORDER BY staff_id;
```

Say: *"Same query, same table — the result depends on who's asking. Rows are
filtered by RLS; sensitive columns are masked or blocked by CLS data policies.
No views, no copies, all declared in Dataform alongside the transformations."*

> If the sales view still shows all rows/raw values, the data-policy IAM may need
> a minute to propagate after the run — re-run the query.

## 7d. (Optional) Data sharing: Analytics Hub hub-and-spoke / NIO (5 min) — live

This maps directly to the NIO deep-dive agenda: **hub owns storage, spokes pay
compute.** Full runbook: [`data-sharing.md`](data-sharing.md).

1. **Architecture alignment** — the `share` stage built curated `shr_*`
   authorized views in `airport_share` over gold/semantic (subscribers get
   products, not base tables).
2. **Publisher workflow (hub / CAG admin)** — create the private exchange +
   listing and whitelist the subscriber:

   ```bash
   source .env && bash scripts/setup_analytics_hub.sh
   ```

   Show, in Analytics Hub: the data exchange, the listing over `airport_share`,
   and the subscriber whitelisted **on the listing** (not project-wide).
3. **Subscriber onboarding (spoke)** — from the subscriber project, link the
   dataset and run a **cost-isolated** query:

   ```bash
   source .env && bash scripts/subscribe_analytics_hub.sh
   ```

   Point out the job ran in the **subscriber** project (`total_bytes_billed`
   there) — storage stayed with the publisher.
4. **Governance & monitoring** — upstream RLS/CLS still applies to the shared
   views; Analytics Hub/BigQuery audit logs show who subscribed and queried.

Say: *"CAG publishes once; each partner (CAAS, SQ, SATS) subscribes into their own
project and pays for their own queries. You keep the data and the governance; they
get a live, read-only product — no copies, no egress of raw rows."*

> Data-owner approval: publish as a **restricted subscription** so requests wait
> for approval (Analytics Hub → listing → Subscriptions → Approve). Whitelisting
> controls who can *request*; approval controls who is *admitted*.

## 8. Close (2 min) — concept

- Transformation (Dataform) vs semantics (views/Looker) — clean separation.
- Spark for messy ingestion, called from Dataform, orchestrated by Composer.
- Governance built in: RLS + CLS/masking (the `security` stage), plus Gemini
  auto-metadata. Optional Pub/Sub baggage streaming is available as a separate
  manual DAG; continuous queries remain the next real-time extension.
- Data sharing beyond BI: Analytics Hub hub-and-spoke (the `share` stage +
  setup/subscribe scripts) — the NIO model, with cost isolation per subscriber.

## Optional: streaming baggage demo (5 min)

Trigger the separate manual DAG for a short run:

```bash
gcloud composer environments run "$COMPOSER_ENV" \
  --location="$REGION" dags trigger -- \
  -r "baggage-stream-demo" airport_ops_baggage_stream_demo
```

Then query the bronze stream table:

```sql
SELECT publish_time, event_id, bag_id, flight_id, scan_type, scan_ts
FROM `your-project-id.airport_bronze.brz_baggage_events_stream`
WHERE publish_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
ORDER BY publish_time DESC
LIMIT 20;
```

Show the silver dedupe view:

```sql
SELECT publish_time, event_id, bag_id, flight_id, scan_type, scan_ts
FROM `your-project-id.airport_silver.slv_baggage_events_stream_deduped`
ORDER BY publish_time DESC
LIMIT 20;
```

Say: *"This is intentionally separate from the batch lakehouse DAG. Pub/Sub owns
the live append into bronze; Dataform picks it up from silver onward for dedupe
and modeling. Schema revisions validate producers, but table evolution stays a
BigQuery storage contract."*

## Anticipated Q&A

- **"How does the column masking actually work — Data Catalog policy tags?"**
  No — the modern **SQL-based `DATA_POLICY`** approach: a `DATA_MASKING_POLICY` and
  a `RAW_DATA_ACCESS_POLICY` are attached to each column via `ALTER COLUMN … SET
  OPTIONS`, and `GRANT FINE_GRAINED_READ` to each group's `principalSet` decides
  who sees raw vs masked. All authored in Dataform SQLX — no policy tags, no
  Terraform.
- **"Does the security stage affect / risk the pipeline?"**
  No. `staff_directory` lives in its own `airport_governance` dataset and **nothing
  in the medallion graph reads it**, so its RLS/CLS policies can't change pipeline
  results. (Applying RLS to a *pipeline* table would also filter the Dataform SA's
  reads — that's a roadmap item with a documented caveat.)
- **"Why ingest feedback as a view over external JSON if it's an anti-pattern?"**
  It's a deliberate teaching contrast: it shows BigQuery's native `JSON` type and
  *why* a non-materialised view over row-oriented external JSON is slow to serve
  (re-scan + re-parse per query). The baggage Parquet/BigLake path is the "good"
  columnar counter-example.
- **"Is any of this real data / PII?"**
  100% synthetic and public-safe — deterministic seed, no real airport, passenger,
  or proprietary data, and no logos.
- **"What does it cost to run?"**
  Small synthetic volumes, partitioned/clustered tables, and a teardown script.
  The priced pieces are the Gemini `AI.GENERATE_TEXT` calls and (optional)
  Gemini-in-BigQuery data insights.
- **"Why Dataform instead of dbt or plain Python?"**
  Dataform is native to BigQuery: a declarative SQL graph with `ref()` ordering,
  built-in assertions, and free lineage/docs — and Spark + Gemini are *called from*
  the same graph. Python/Spark is still used where the work isn't SQL-shaped
  (gzip/nested ingestion), just wrapped as Dataform `CALL`s.
- **"Why is orchestration separate from transformation?"**
  Composer = **when** (scheduling, retries, alerts); Dataform = **what** (models,
  order, tests, lineage); Spark = **how** for messy ingestion; BigQuery = **where**
  compute happens. Clean separation of concerns.
- **"If our analysts use BigQuery Data Prep / Pipelines, how does that fit — who
  owns what, and how does it get into this repo?"**
  Same Dataform engine, different front door: code + Composer (engineers) vs the
  visual UI + Dataform-native cron (analysts). Engineers own the repo lifecycle,
  release/workflow configs, IAM and ingestion (bronze/silver); analysts author
  the gold/semantic business logic. **Promotion = a PR:** you take the SQL the UI
  generated and land it as a reviewed SQLX/declaration in the engineering repo,
  where it becomes a normal `ref()` node. Default posture is **Consolidate** — one
  PR-protected repo, analysts get workspaces in it.
- **"What stops an analyst scheduling a pipeline off a random CSV in their own
  playground?"**
  Governance, not magic. A Dataform model can only read data via a reviewed
  `type: "declaration"` source, so an undeclared CSV simply isn't a production
  input. The risk is real though: the UI's "create pipeline" spins up a *separate*
  Dataform repo with its own schedule (not our Composer) and, across repos, you
  only get a cross-repo `declaration` (a reference, **not** automatic ordering).
  That's exactly why we Consolidate into one repo and discourage shadow pipelines.
  Full detail in `docs/design-philosophy.md` (Part 3) and `docs/architecture.md`
  (Two-repo design → third path).
- **"Where does Data Canvas fit — isn't it just an AI notebook? And how do Canvas,
  notebooks and Pipelines chain together?"**
  Data Canvas is the AI **explore/ask** surface (natural-language, DAG-based) — a
  *sibling* to notebooks, **not** Dataform-backed, and not for business users. The
  supported chain is real: **Canvas (explore) → Export as notebook (tidy) → import
  that notebook into a BigQuery Pipeline + add SQL tasks (sequence) → schedule**
  (Dataform-native cron). A Pipeline runs both SQL and notebook tasks; importing a
  notebook makes a **copy** (the source isn't linked, so edits don't auto-sync).
  That's the **analyst loop**, all inside BigQuery Studio. Our demo is the
  **engineering loop** — Git `.sqlx` + Composer. Same Dataform engine; the bridge
  is promotion (a PR). See the "Analyst loop vs engineering loop" slide and
  `docs/design-philosophy.md` (Part 3).

## Teardown (after)

```bash
source .env && bash scripts/teardown.sh
```
