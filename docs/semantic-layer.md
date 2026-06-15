# Gold layer vs. the semantic layer (and where Dataform fits)

This note explains the design decision behind the gold layer and answers the
"Gold Layer vs Headless Semantic Layer" question directly. It is written so it
can be used as workshop talking points.

## TL;DR

- **A semantic layer is a query generator, not a transformation engine.** It
  takes a request ("daily delay rate by terminal") and generates SQL against a
  clean model at query time. It does **not** clean, join-to-conform, or reshape
  raw data.
- **Heavy transformation belongs in the ELT layer (Dataform).** Cleaning,
  typing, deduplication, conforming, CDC, business rules — all done once, in
  Dataform, and materialised as an **atomic star schema**.
- **The mart should be at the finest grain possible.** Roll-ups are the semantic
  layer's job, computed on read. Don't pre-aggregate into report-specific tables.

This is exactly the architecture in this repo:

```
Dataform (transform)                         Semantic layer (roll-up)
--------------------                         ------------------------
bronze -> silver -> gold (atomic star)  -->  airport_semantic.* VIEWS
                                             (or Looker / AtScale / Cube)
```

## Why not pre-aggregated gold marts?

A common (legacy) pattern is to build gold as physically pre-aggregated,
denormalised, use-case-specific tables (one table per dashboard). The problem:

- **It bakes business logic into rigid pipelines.** Every new question needs a
  new pipeline / table. That is the "request → wait → build" cycle.
- **It destroys grain.** Once you roll baggage events up to hourly summaries,
  the atomic detail (timestamps, variance) is gone — AI/ML and ad-hoc
  root-cause analysis hit a structural wall.
- **A semantic layer can't do its job over it.** Headless engines map metrics
  over **granular, conformed star schemas**. Map them over already-aggregated
  tables and you lose dynamic grain, metric inheritance, and the universal API.

So this demo models gold as an **atomic conformed star schema** and pushes
roll-ups into a logical view layer. That keeps the platform agile and makes it
ready for a real semantic layer.

## What we actually built

**Gold = atomic star schema** (`airport_gold`):

| Table | Grain | Notes |
|---|---|---|
| `dim_terminal`, `dim_airline`, `dim_date` | one row per entity | conformed dimensions |
| `fct_flight` | one row per flight | partitioned by date, clustered by terminal/airline |
| `fct_baggage` | one row per bag | orphans quarantined |
| `fct_feedback` | one row per feedback | Gemini-derived attributes |

**Semantic layer = views** (`airport_semantic`), computed at query time, nothing
materialised:

- `sem_airport_operations_daily` — daily KPIs rolled up from `fct_flight` + `fct_baggage`
- `sem_terminal_performance_hourly` — terminal × hour congestion
- `sem_passenger_experience` — sentiment/urgency roll-up from `fct_feedback`

Open any of these views and you will see it is **just a `GROUP BY` over the
atomic facts**. That is the whole idea: the metric is defined once, logically,
and generated on read.

## The view is a "mini semantic layer" — swap it for a real one

A BigQuery view illustrates the principle (logical, query-time, no copy of the
data). A production semantic layer adds:

- reusable **metric definitions** (single source of truth) consumed by every BI
  tool and AI agent,
- **caching / aggregate awareness** for performance,
- **governance** and a **universal API**.

You can replace the `airport_semantic` views with:

- **Looker / LookML** — Google's semantic layer (note: this is a licensed
  product; consuming it from Tableau/Power BI via the Open SQL Interface needs at
  least a Looker *Standard* user because the connector requires the `explore`
  permission),
- **Cube / AtScale** — headless semantic layers,

…and the gold star schema underneath does not change. That is the payoff of
keeping transformation and semantics in separate layers.

## One nuance to be precise about

"The semantic layer never materialises anything" is *almost* true. For
performance, engines may cache or build aggregate tables (BigQuery materialized
views, BI Engine, Looker aggregate awareness / PDTs). The rule is: **logic is
defined once in the semantic layer; the engine may cache it — but you do not
hand-build report-specific physical tables in the warehouse.**

## How this maps to the CAG EDP 2.0 discussion

- "Consumption layer = separate aggregated / denormalised tables" → replaced by
  **atomic star schema + logical semantic views**. ✔ agrees with the
  headless-semantic-layer position.
- "Star schema vs data mart" → gold here is a **structural star schema**
  (Kimball facts + conformed dimensions), reusable across use cases, not a
  single-purpose reporting table. ✔
- Dataform is **not** the semantic layer; it is the analytics-engineering layer
  that produces the clean atomic models the semantic layer sits on. ✔
