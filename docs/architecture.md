# Architecture

```mermaid
flowchart LR
  subgraph GCS[Cloud Storage raw landing]
    A1[flight_schedules CSV]
    A2[flight_events JSONL]
    A3[baggage_events Parquet]
    A4[passenger_flow CSV.gz]
    A5[security_wait_times nested JSON]
    A6[customer_feedback multilingual JSON]
  end

  subgraph DF[Dataform - transformation]
    direction TB
    OPS[operations: native loads, BigLake ext table, Spark procs CALL, Gemini model]
    BRZ[bronze: typed + ingestion metadata]
    SLV[silver: conformed + Gemini enrichment]
    GOLD[gold: ATOMIC star schema]
    OPS --> BRZ --> SLV --> GOLD
  end

  subgraph SPARK[Serverless BigQuery Spark stored procedures]
    SP[gz CSV / nested JSON / feedback JSON]
  end

  subgraph SEM[Semantic layer - query-time roll-up]
    V1[sem_airport_operations_daily]
    V2[sem_terminal_performance_hourly]
    V3[sem_passenger_experience]
  end

  A1 --> OPS
  A2 --> OPS
  A3 --> OPS
  A4 --> SP --> OPS
  A5 --> SP
  A6 --> SP
  GEM[BigQuery ML remote model over Gemini] --> SLV
  GOLD --> V1
  GOLD --> V2
  GOLD --> V3
  GOLD --> ASSERT[assertions / data quality]

  COMPOSER[Cloud Composer DAG] -. compiles + invokes by tag .-> DF
  DF -. lineage .-> DPLX[Dataplex / BigQuery lineage]
```

## Layer responsibilities

| Layer | Tech | Owns |
|---|---|---|
| Orchestration | Cloud Composer (Airflow) | Outer DAG, scheduling, stage sequencing, run summary |
| Transformation | Dataform | SQL graph, bronze/silver/gold, assertions, docs, lineage |
| Heavy ingestion | BigQuery Spark stored procedures | gzip CSV, nested JSON, multilingual JSON |
| AI enrichment | BigQuery ML remote model (Gemini) | translate + classify feedback |
| Storage/compute | BigQuery + BigLake + Cloud Storage | tables, external tables, raw files |
| Semantic layer | BigQuery views (→ Looker/AtScale/Cube) | query-time roll-up, metric definitions |
| Governance | Dataplex / BigQuery lineage, assertions | lineage, quality, (Phase 2: RLS/CLS) |

## Datasets (region us-central1)

| Dataset | Contents |
|---|---|
| `airport_ops_control` | raw landing tables (loaded by ops/Spark) + Spark procedures |
| `airport_bronze` | typed bronze tables with ingestion metadata |
| `airport_silver` | conformed silver models + Gemini-enriched feedback |
| `airport_gold` | atomic star schema (dims + facts) + data quality summary |
| `airport_semantic` | semantic roll-up views |
| `airport_ai` | Gemini remote model |
| `dataform_assertions` | assertion results |

## Two-repo layout

- `airport-ops-lakehouse-demo` (this repo): docs, data generator, Spark reference
  code, Composer DAG, scripts, infra.
- `airport-ops-lakehouse-dataform`: the Dataform project at repo root, connected
  to the GCP Dataform repository and invoked by Composer.

See [`semantic-layer.md`](semantic-layer.md) and
[`why-dataform-not-python.md`](why-dataform-not-python.md) for the design
rationale, and [`demo-script.md`](demo-script.md) for the runbook.
```
