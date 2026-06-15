# Why Dataform (and not "just Python")?

A frequent question: *"What is Dataform, and why not just write Python?"* Both
can move data, but they solve different problems. For SQL-based transformation in
BigQuery, Dataform gives you things you would otherwise hand-build in Python.

## The one-liner

> Dataform is a **declarative** analytics-engineering framework: you describe the
> tables you want and how they relate; Dataform figures out the order, runs the
> SQL in BigQuery, tests it, documents it, and version-controls it. Python is
> **imperative**: you write *how* to do every step yourself.

## Side by side

| Concern | Python script | Dataform |
|---|---|---|
| Execution model | Imperative — you order every step | Declarative — dependency graph (`ref()`) auto-orders |
| Where compute runs | Wherever Python runs (pull data out?) | Pushed down into BigQuery (no data movement) |
| Dependencies | You track them manually | Derived automatically from `ref()` |
| Re-runs / idempotency | You implement it | Built in (`CREATE OR REPLACE`, incremental) |
| Data quality tests | You write a framework | Built-in assertions (`uniqueKey`, `nonNull`, custom) |
| Lineage | You build it | Generated from the graph |
| Documentation | Separate, drifts | Inline with the model, versioned |
| Reusability (DRY) | Functions, if you design them | `includes/` JS (metrics, constants, prompts) |
| Version control / review | Yes, but ad hoc | Git-native, compile-checked |

## What "declarative" buys you (from this demo)

- `ref("brz_flight_schedules")` — Dataform knows `slv_flights` depends on the
  bronze table and schedules it after. You never write the order.
- `includes/metrics.js` — the delay-bucket and SLA logic is written **once** and
  referenced by silver and gold. Change the SLA threshold in one place.
- `assertions` — `uniqueKey: ["flight_id"]` is a test that runs every pipeline
  run. In Python you would build and maintain that yourself.
- The dependency graph is the lineage and the documentation, for free.

## When Python (or Spark) *is* the right tool

Dataform orchestrates **SQL**. When the work isn't SQL-shaped — parsing
compressed/nested files, complex procedural logic, ML feature code — use the
right engine and **call it from Dataform**. This demo does exactly that:

- gzip CSV, nested JSON, and multilingual JSON are processed by **serverless
  BigQuery Spark stored procedures** (PySpark),
- those procedures are **wrapped as Dataform operations** (`CALL ...`),
- and the whole graph is **invoked by Cloud Composer (Airflow)**.

So it is not "Dataform vs Python." It is: **Python/Spark for the messy
procedural ingestion, Dataform for the governed SQL transformation graph,
Composer for the outer orchestration.** Each tool does what it is best at.

## Mental model

```
Composer (Airflow)   = WHEN things run (scheduling, outer DAG, retries, alerts)
Dataform             = WHAT the SQL models are + their order, tests, docs, lineage
Spark stored procs   = HOW the non-SQL/heavy ingestion is done
BigQuery             = WHERE compute + storage happen
```
