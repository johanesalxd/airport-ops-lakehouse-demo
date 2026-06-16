# Design philosophy

This is the *why* behind the data model. Two ideas matter most, and they are the
ones most often confused:

1. **The medallion architecture** — why data flows through bronze → silver → gold.
2. **Gold is an atomic star schema, NOT a semantic layer** — and why those are
   different things that belong in different places.

If you read one doc to understand the demo's design decisions, read this one.

---

## Part 1 — The medallion architecture

A **medallion (multi-hop) architecture** organises data into progressively
refined layers. Each layer has one job, and data only ever flows one way.

```
raw files → BRONZE → SILVER → GOLD → (semantic layer)
```

As defined by both [Google Cloud](https://cloud.google.com/discover/what-is-medallion-architecture)
and [Databricks](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion),
the layers map to data *quality and structure*: bronze = raw ingestion, silver =
cleaned/validated/conformed, gold = dimensional modelling and business-ready
consumption. Following it is a recommended best practice, not a requirement.

### Bronze — "land it faithfully"

- **Job:** capture the source data as-is, with **ingestion metadata**
  (`_batch_id`, `_source_file`, `_source_format`, `_ingested_at`, `_record_hash`)
  — the docs call for provenance columns such as `_metadata.file_name` here.
- **Rule:** do *not* apply business logic here. Bronze is the reproducible,
  auditable record of what arrived. If a downstream layer is wrong, you can
  always rebuild it from bronze without re-reading the source.
- **In this demo:** `airport_bronze.brz_*`, one per source, fed by native loads,
  the BigLake external table, and the Spark stored procedures. Note we *type*
  bronze (a BigQuery-flavoured choice for a clean demo); the strictest reading of
  the pattern keeps fields as raw strings/`VARIANT` to survive schema drift. We
  preserve raw fidelity either way — no rows are dropped or reshaped.
- **One deliberate exception — `brz_customer_feedback`:** it is a **view**, not a
  table, sitting directly over a plain external table that maps the feedback
  NDJSON to a single native `JSON` column. This shows off BigQuery's `JSON` type
  (query `payload.feedback_text` with no parsing step) — but it's a teaching
  **anti-pattern for serving**: a non-materialised view over external,
  row-oriented JSON re-reads and re-parses the raw text on every query, with no
  column pruning, unlike a materialised native table or a columnar format
  (Parquet, as the baggage source uses). "Just because you can, doesn't mean you
  should." Production: materialise it like the other bronze tables.

### Silver — "make it trustworthy and conformed"

- **Job:** clean, type, deduplicate, validate, and **conform** — standardise
  keys and codes so tables can be joined. This is where business rules and
  enrichment live.
- **In this demo:** `airport_silver.slv_*` — e.g. `slv_flights` (one row per
  flight occurrence), `slv_flight_delays` (delay buckets via shared logic),
  `slv_baggage_journey`, and `slv_customer_feedback_enriched` (the Gemini
  translation/classification step). Anomalies are **quarantined** here so they
  don't corrupt gold but remain visible.

### Gold — "model it for consumption"

- **Job:** the business-facing model. Conformed, reusable, and modelled — here as
  a **Kimball star schema** (see Part 2).
- **In this demo:** `airport_gold` — `dim_terminal`, `dim_airline`, `dim_date`,
  `fct_flight`, `fct_baggage`, `fct_feedback`, plus `gold_data_quality_summary`.

### Why bother with three layers?

- **Separation of concerns** — ingestion problems, data-quality problems, and
  modelling problems are debugged in different places.
- **Reproducibility** — each layer is rebuildable from the one before it.
- **Reusability** — many silver/gold consumers share one bronze ingestion; you
  don't re-parse the raw file for every use case.
- **Lineage & trust** — the layer boundaries are the lineage story.

---

## Part 2 — Gold as an atomic star schema (and where aggregation lives)

This is the design decision people argue about, so it is worth being precise.
It also answers the "Gold Layer vs Headless Semantic Layer" critique directly.

### What the canonical definition actually says

Per the docs, the gold layer does **two** things: **dimensional modelling**
*and* **aggregation**.

- Databricks: *"the gold layer models your data for reporting and analytics using
  a dimensional model"* and *"consists of aggregated data tailored for analytics
  and reporting."*
- Google Cloud: the gold layer *"utilises **both** aggregated marts and star
  schemas."*

So a star schema in gold is canonical, and so are pre-aggregated marts. Both are
valid. The question is not "which one is correct" — it is **where you let
aggregation live.**

### Our deliberate choice

We keep gold at **atomic, dimensional (star-schema) grain** and **delegate
aggregation to a separate semantic layer**, instead of materialising
pre-aggregated, one-table-per-dashboard marts in gold.

```
Dataform (transform)                         Semantic layer (roll-up)
--------------------                         ------------------------
bronze → silver → gold (atomic star)   -->   airport_semantic.* VIEWS
                                             (or Looker / AtScale / Cube)
```

This is an opinionated stance, aligned with the **headless semantic layer**
position. The reasoning:

- **A semantic layer is a query generator, not a transformation engine.** It
  takes a request ("daily delay rate by terminal") and generates SQL against a
  clean model *at query time*; it does not clean, conform, or reshape data. That
  job stays in Dataform (cleaning, typing, dedup, conforming, business rules —
  done once, materialised as the atomic star schema).
- **Keeping the finest grain keeps options open.** Pre-aggregating to, say,
  hourly summaries discards the atomic detail (timestamps, variance) that AI/ML
  and ad-hoc root-cause analysis need. You can always roll up from atomic; you
  cannot drill down from a pre-aggregate.
- **It keeps the platform agile.** With pre-aggregated marts, every new question
  tends to need a new pipeline/table — the "request → wait → build" cycle.
- **It is semantic-layer ready.** Headless engines map metrics over granular,
  conformed star schemas; mapping them over already-aggregated tables loses
  dynamic grain, metric inheritance, and the universal API.

To be clear about the trade-off: the canonical pattern of materialising aggregates
in gold (e.g. a `weekly_sales` materialized view) is perfectly valid and is often
done for performance. We push that responsibility down into the semantic layer
(and, in production, let it cache/aggregate-aware as needed) so the warehouse
keeps one atomic source of truth rather than many report-specific copies.

### What "atomic star schema" means here

| Table | Grain | Notes |
|---|---|---|
| `dim_terminal`, `dim_airline`, `dim_date` | one row per entity | conformed dimensions |
| `fct_flight` | one row per flight | partitioned by date, clustered by terminal/airline |
| `fct_baggage` | one row per bag | orphans quarantined |
| `fct_feedback` | one row per feedback | Gemini-derived attributes |

These are **structural** facts and dimensions (reusable across use cases), not a
single-purpose reporting table.

### The semantic layer = views (a "mini semantic layer")

`airport_semantic` is three BigQuery **views**, computed at query time, nothing
materialised:

- `sem_airport_operations_daily` — daily KPIs from `fct_flight` + `fct_baggage`
- `sem_terminal_performance_hourly` — terminal × hour congestion
- `sem_passenger_experience` — sentiment/urgency roll-up from `fct_feedback`

Open any of them: it is **just a `GROUP BY` over the atomic facts**. That is the
whole idea — the metric is defined once, logically, and generated on read.

A BigQuery view illustrates the *principle* (logical, query-time, no copy of the
data). A production semantic layer adds reusable **metric definitions** (one
source of truth for every BI tool and AI agent), **caching / aggregate
awareness**, **governance**, and a **universal API**. You can replace the
`airport_semantic` views with:

- **Looker / LookML** — Google's semantic layer (a licensed product; consuming it
  from Tableau/Power BI via the Open SQL Interface needs at least a Looker
  *Standard* user, because the connector requires the `explore` permission),
- **Cube / AtScale** — headless semantic layers,

…and the gold star schema underneath **does not change**. That is the payoff of
keeping transformation and semantics in separate layers.

### One nuance, to be precise

"The semantic layer never materialises anything" is *almost* true. For
performance, engines may cache or build aggregates (BigQuery materialized views,
BI Engine, Looker aggregate awareness / PDTs). The rule is: **logic is defined
once in the semantic layer; the engine may cache it — but you do not hand-build
report-specific physical tables in the warehouse.**

### And Dataform is not the semantic layer

Dataform is the **analytics-engineering / transformation** layer that produces
the clean atomic models the semantic layer sits on. It is not a metrics engine.
For what Dataform *is* (and when to use Spark/Python instead), see
[`why-dataform-not-python.md`](why-dataform-not-python.md).

---

## Part 3 — Two doors to one engine (engineer-authored vs analyst-authored)

A recurring source of confusion: **BigQuery Data Pipelines** and **BigQuery
Data Preparation** (the visual, Gemini-assisted tools in BigQuery Studio) look
like a *different* product from the Dataform this demo is built on. They are not.
They are **the same Dataform engine behind a different front door.**

| | **Engineer door** (this demo) | **Analyst door** (BQ Pipelines / Data Prep) |
|---|---|---|
| Authoring | Hand-written `.sqlx` in a Git repo | Visual, low-code, Gemini suggestions in BigQuery Studio |
| Engine | **Dataform** | **Dataform** (same engine) |
| Orchestrator | **Cloud Composer** (Airflow DAG) | **Dataform-native cron** (a *workflow configuration*) |
| Persona | Data engineer | Data analyst doing light data engineering |
| Best for | Cross-service, code-reviewed, reaches outside BigQuery | SQL-shaped transforms that stay inside BigQuery |

So Data Prep / Pipelines is the **analyst's low-code on-ramp** to Dataform — and
on the medallion, its natural home is the **silver-tail → gold → semantic** end
(business logic, joins, KPIs, roll-ups), *not* the messy ingestion end (gzip,
nested JSON, Spark, external/BigLake, governance), which stays engineer territory.

> **The deciding factor is persona + scope of dependencies — not the medallion
> layer.** Analyst, stays inside BigQuery, wants visual help → analyst door.
> Engineer, version-controlled, reaches outside BigQuery → engineer door.

### Consolidate vs Federate (the governance decision)

The analyst door is permissive: clicking "create pipeline" in BigQuery Studio
provisions a **brand-new, separate Dataform repository** with its **own schedule
and service account** — *not* part of this repo and *not* run by our Composer.
Left ungoverned, that fragments fast (shadow repos, ad-hoc schedules off random
CSVs, no PR history, scattered logs). There are two valid postures:

| | **Option 1 — Consolidate (production default)** | **Option 2 — Federate (deliberate exception)** |
|---|---|---|
| Where analysts work | A **workspace inside the engineering repo** | Their **own** Dataform repo |
| Dependencies | Real `ref()` (one compilation graph) | Cross-repo **`declaration`** only (reference, not ordering) |
| Scheduling | Engineering-owned (Composer, or tag-scoped workflow configs) | The analyst repo's own Dataform cron |
| Promotion | Merge a PR → it's a normal graph node | N/A — stays separate by design |
| Use when | Output feeds production / other teams depend on it | Self-contained, analyst-local mart that should *not* couple to the engineering release train |
| Cost | Requires repo discipline (PRs, reviews) | Accepts a manual cross-repo seam + duplicate logs |

**Recommendation:** default to **Consolidate** — one PR-protected repo, analysts
get workspaces in it, every input is a reviewed `declaration`, and prod is gated
behind release + workflow configs the engineers own. Use **Federate** only as a
conscious choice for genuinely independent analyst marts, never by accident.

### The analyst loop vs the engineering loop

Step back from individual tools and there are **two end-to-end loops** for
getting from a question to a scheduled, governed asset. BigQuery Studio ships a
whole family of Gemini-assisted tools that together form the **analyst loop**;
this demo is the **engineering loop**.

The BigQuery Studio AI family, in order of use:

1. **Data Canvas** — *explore / ask.* A natural-language, DAG-based analysis
   surface (find tables, generate SQL, chart, summarize). It is **not**
   Dataform-backed and is a *sibling* to notebooks, not a notebook itself. Best
   for throwaway-friendly exploration. From a SQL node you can **export to** a
   notebook, a scheduled query, or a Looker Studio report.
2. **Notebook** — *tidy / package.* The exported Canvas work becomes a Colab
   Enterprise notebook (Python + SQL) you clean up into a repeatable unit.
3. **Data Preparation** — *clean / transform.* Gemini-assisted, low-code
   cleanup (typecast, standardize, enrich, schema-map). Powered by Dataform.
4. **Data Pipelines** — *sequence / schedule.* Ties **SQL tasks and notebook
   tasks** together in dependency order on a cron. Powered by Dataform. You can
   **Add task → Notebook** and *import an existing notebook* — note the import
   makes a **copy** (the source notebook is unchanged; edits don't auto-sync).

So the full analyst chain is real and supported end to end:

> **Data Canvas (explore)** → **Export as notebook (tidy)** → **import the
> notebook into a Pipeline + add SQL tasks (sequence)** → **schedule it**
> (Dataform-native cron). All low-code, all inside BigQuery Studio, no Git, no
> Composer.

| | **Analyst loop** | **Engineering loop** (this demo) |
|---|---|---|
| Explore | Data Canvas (NL, Gemini) | Ad-hoc SQL / notebooks |
| Author | Data Prep + notebooks (low-code) | Hand-written `.sqlx` in Git |
| Assemble | BigQuery Pipeline (SQL + notebook tasks) | Dataform compilation graph (`ref()`) |
| Version control | Optional / behind the UI | **Git, PR-reviewed** |
| Orchestrate | **Dataform-native cron** (workflow config) | **Cloud Composer** (Airflow DAG) |
| Engine | **Dataform** (+ notebook runtimes) | **Dataform** (+ Spark, Gemini) |
| Reaches outside BigQuery? | No (stays in BigQuery Studio) | Yes (Spark ingestion, Gemini, governance) |
| Persona | Data analyst | Data engineer |
| Best for | Fast, self-service insight & analyst marts | Governed, cross-service production pipelines |

**Both loops run on the same Dataform engine.** The bridge between them is
**promotion**: when an analyst-loop asset needs to become governed production,
you re-land its SQL as a reviewed SQLX/declaration **PR** into the engineering
repo (the Consolidate path above) — or, deliberately, leave it in its own repo
consuming `gold` via a `declaration` (Federate). Choose by whether production
depends on it.

The concrete repo mechanics (separate repo vs single-repo tags + workflow
configs, cross-repo declarations, and how to *promote* a UI-built pipeline into
this repo) are in
[`architecture.md` → Two-repo design](architecture.md#two-repo-design). The
dev → prod SDLC model is in [`roadmap.md` → CI/CD & environments](roadmap.md).

Reference docs:
[Data canvas](https://docs.cloud.google.com/bigquery/docs/data-canvas) ·
[BigQuery pipelines](https://docs.cloud.google.com/bigquery/docs/pipelines-introduction) ·
[Data preparation](https://docs.cloud.google.com/bigquery/docs/data-prep-introduction) ·
[Manage the Dataform code lifecycle](https://docs.cloud.google.com/dataform/docs/managing-code-lifecycle) ·
[Schedule runs (workflow configurations)](https://docs.cloud.google.com/dataform/docs/schedule-runs) ·
[Declare a data source](https://docs.cloud.google.com/dataform/docs/declare-source).

---

## Sources

- Google Cloud — *What is medallion architecture?*
  https://cloud.google.com/discover/what-is-medallion-architecture
- Google Cloud — *Lakehouse key concepts*
  https://docs.cloud.google.com/lakehouse/docs/key-concepts
- Databricks / Azure — *What is the medallion lakehouse architecture?*
  https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion
- Databricks — *What is Medallion Architecture?*
  https://www.databricks.com/blog/what-is-medallion-architecture
