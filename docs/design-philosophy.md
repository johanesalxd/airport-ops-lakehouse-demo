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

### Bronze — "land it faithfully"

- **Job:** capture the source data as-is, with typing and **ingestion metadata**
  (`_batch_id`, `_source_file`, `_source_format`, `_ingested_at`, `_record_hash`).
- **Rule:** do *not* apply business logic here. Bronze is the reproducible,
  auditable record of what arrived. If a downstream layer is wrong, you can
  always rebuild it from bronze without re-reading the source.
- **In this demo:** `airport_bronze.brz_*`, one table per source, fed by native
  loads, the BigLake external table, and the Spark stored procedures.

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

## Part 2 — Gold is a star schema, NOT a semantic layer

This is the design decision people argue about, so it is worth being precise.
It also answers the "Gold Layer vs Headless Semantic Layer" critique directly.

### TL;DR

- **A semantic layer is a query generator, not a transformation engine.** It
  takes a request ("daily delay rate by terminal") and generates SQL against a
  clean model *at query time*. It does **not** clean, conform, or reshape data.
- **Heavy transformation belongs in the ELT layer (Dataform).** Cleaning, typing,
  dedup, conforming, business rules — done once, materialised as an **atomic star
  schema** in gold.
- **The mart should be at the finest grain possible.** Roll-ups are the semantic
  layer's job, computed on read. Don't pre-aggregate into report-specific tables.

```
Dataform (transform)                         Semantic layer (roll-up)
--------------------                         ------------------------
bronze → silver → gold (atomic star)   -->   airport_semantic.* VIEWS
                                             (or Looker / AtScale / Cube)
```

### Why not pre-aggregated gold marts?

A common legacy pattern builds gold as physically pre-aggregated, denormalised,
one-table-per-dashboard marts. The problems:

- **It bakes business logic into rigid pipelines.** Every new question needs a
  new pipeline/table — the "request → wait → build" cycle.
- **It destroys grain.** Once baggage events are rolled up to hourly summaries,
  the atomic detail (timestamps, variance) is gone — AI/ML and ad-hoc root-cause
  analysis hit a wall.
- **A semantic layer can't sit on top of it.** Headless engines map metrics over
  **granular, conformed star schemas**. Map them over already-aggregated tables
  and you lose dynamic grain, metric inheritance, and the universal API.

So gold here is an **atomic conformed star schema**, and roll-ups are pushed into
a logical view layer. That keeps the platform agile and ready for a real semantic
layer.

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
