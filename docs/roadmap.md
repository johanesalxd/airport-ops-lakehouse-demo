# Roadmap

The MVP is a clean, governed batch lakehouse — and it already includes optional
showcases for fine-grained-access **governance** (RLS/CLS, §1), **streaming
ingestion** (§2), and **automated data insights** (§4). The items below separate
implemented showcases from remaining natural next steps, kept out of the
critical path so the core demo stays simple. Each is grounded in current Google
Cloud capabilities, with the relevant official Google Cloud documentation linked
inline at each item.

> **Already implemented (no longer roadmap):** §1 Governance (RLS/CLS &
> masking), §2 Streaming ingestion (Pub/Sub to BigQuery), and §4 Automated
> metadata (data insights). They are kept in this doc, marked *Implemented*, for
> context.

## 1. Governance: row-/column-level security & masking

Fine-grained access control as a governance story (synthetic data only — no real
PII).

**Implemented** as a self-contained showcase: a `security` Dataform stage builds
`airport_governance.staff_directory` (a synthetic staff roster that *nothing* in
the medallion pipeline reads, so its policies can't affect the main flow) and
attaches:

- **Row-level security (RLS):** `CREATE ROW ACCESS POLICY` — the admin group sees
  all rows; the sales group sees only `department = 'Sales'`.
- **Column-level security (CLS) + dynamic masking:** the modern SQL-based
  `DATA_POLICY` approach — dual policies per column (`DATA_MASKING_POLICY` +
  `RAW_DATA_ACCESS_POLICY`) attached via `ALTER COLUMN ... SET OPTIONS`, with
  `GRANT FINE_GRAINED_READ` deciding who sees raw vs masked: `ssn`→SHA256,
  `email`→default value, `salary`→NULL, and `bank_account` blocked entirely for
  non-admins.

> **Implementation note (updated):** BigQuery's *newer* SQL-based `DATA_POLICY`
> column masking is authored **entirely in Dataform SQLX** — no policy tags,
> Terraform, or out-of-band `bq`/API steps. (The older Data-Catalog *policy-tag*
> mechanism did require those; this demo uses the SQL approach instead.) The
> execution SA needs `bigquery.dataOwner` (RLS) + `bigquerydatapolicy.admin`
> (data policies); group members need `bigquery.filteredDataViewer` +
> `bigquery.jobUser` (granted by `bootstrap.sh`).

Still open as future extensions:

- Apply the same patterns to **pipeline** tables (e.g. RLS by `terminal_id` on
  `fct_feedback` / `sem_terminal_performance_hourly`) — note this also requires
  granting the Dataform execution SA a permissive row policy so its downstream
  models don't read zero rows.
- **Authorized views:** expose selected gold/semantic objects to BI users while
  preserving RLS/CLS underneath.

### Managed data quality (Dataplex auto data quality + profiling)

Today data quality is enforced *in-pipeline* with Dataform assertions (real gates
in the `quality` stage). Complement — don't replace — that with **managed,
scheduled** quality as a platform capability:

- Run a **data profiling scan** on the gold tables, then use the
  profile to **recommend data-quality rules** (`generateDataQualityRules`).
- Schedule a **data quality scan** (Dataplex/Knowledge Catalog) with those rules,
  publish scores to the BigQuery/Knowledge Catalog metadata pages, and alert on
  failures.

This gives a governance team owned, catalog-visible DQ on top of the developer's
in-graph assertions.

## 2. Streaming ingestion: baggage events via Pub/Sub

**Implemented as an optional showcase** for the ingestion path. Baggage scans can
now be published as low-rate live events:

```
manual Composer simulator DAG
  → Pub/Sub topic: baggage-events
  → Pub/Sub BigQuery subscription
  → airport_bronze.brz_baggage_events_stream
  → Dataform silver view: slv_baggage_events_stream_deduped
```

The showcase uses a **Pub/Sub BigQuery subscription** (writes directly to
BigQuery via the Storage Write API, no subscriber client). Notes:

- BigQuery subscriptions are **at-least-once** — dedupe downstream by event/scan
  id.
- Configure a dead-letter topic for schema/write failures.
- Pub/Sub schema revisions validate producer messages; BigQuery table evolution
  remains a separate, deliberate storage contract.
- For exactly-once or heavy windowed transforms, add a Dataflow Pub/Sub→BigQuery
  pipeline later.
- For CDC-style updates, Pub/Sub BigQuery subscriptions can drive BigQuery CDC
  with `_CHANGE_TYPE` / `_CHANGE_SEQUENCE_NUMBER`.

See [`streaming-ingestion.md`](streaming-ingestion.md) for the runbook, schema
versioning, and replay/backfill guidance.

## 3. Real-time analytics: BigQuery continuous queries

Once events stream in:

1. **Real-time baggage SLA** — a continuous query over
   `APPENDS(TABLE airport_bronze.brz_baggage_events_stream, …)` writing
   `airport_realtime.baggage_sla_realtime`, flagging missing/late scans.
2. **Operational alerts** — a continuous query filtering severe exceptions and
   `EXPORT DATA` to a Pub/Sub topic for downstream alerting.
3. **Real-time Gemini enrichment** — optional; call `AI.GENERATE_TEXT` over new
   rows. Keep optional: continuous AI calls get expensive.

Caveats: continuous queries are long-running jobs — document start/stop and cost
controls; verify the launch stage of stateful operations before relying on them.

## 4. Automated metadata: BigQuery data insights

**Implemented** as an optional script — [`scripts/generate_data_insights.sh`](../scripts/generate_data_insights.sh).
It uses Gemini-in-BigQuery via Dataplex `DATA_DOCUMENTATION` data scans to
auto-generate metadata over the built layers:

```
for each table in silver + gold:
  → run a DATA_PROFILE scan + publish it (grounds the insights)
  → run a DATA_DOCUMENTATION scan (generationScopes=ALL, catalogPublishingEnabled)
  → publish table/column descriptions + suggested questions/SQL to Knowledge Catalog
for each view in semantic:
  → run a DATA_DOCUMENTATION scan (document only; views can't be profiled)
optionally (--dataset-insights):
  → dataset-level DATA_DOCUMENTATION scan → relationship graph + cross-table queries
```

Generates table/column descriptions, suggested questions + SQL, and (Preview)
dataset relationship graphs. Caveats: dataset insights are Preview; data insights
are Gemini-in-BigQuery features with their own pricing; regenerating overwrites
previous insights; `GEO`/`JSON` columns and >350 columns/table aren't supported;
external/BigLake tables need the scan/connection SA to have GCS read on the
underlying bucket (our gold/silver targets are native, so this isn't required).

Future: wrap it in a second Composer DAG that runs after the lakehouse build, so
metadata refresh is orchestrated alongside the pipeline rather than run manually.

## 5. Conversational analytics / data agents

Put a natural-language layer on top of the `airport_semantic` views so an ops user
can simply *ask* — "which terminal had the worst delay rate yesterday?" — instead
of writing SQL:

- Create a **BigQuery data agent** whose knowledge sources are the semantic views,
  with context/instructions and **verified ("golden") queries** for the common
  ops questions.
- Or call the **Conversational Analytics API** (`geminidataanalytics.googleapis.com`)
  to embed a chat experience in an app.

This is the AI-native consumption surface that sits beside the BI dashboard.
Caveats: conversational analytics is **Preview**, operates globally (no region
choice), and is billed at BigQuery compute pricing for the queries it runs.

## 6. AI extension: vector search & embeddings

Go beyond text generation on feedback to **semantic search and RAG**:

- Generate embeddings over `feedback_text` / `english_translation` with
  `AI.GENERATE_EMBEDDING` (or autonomous embedding generation), optionally build a
  `CREATE VECTOR INDEX`, and search with `VECTOR_SEARCH` / `AI.SEARCH`.
- Use cases: "find feedback similar to this complaint", theme clustering, or RAG
  that grounds a summary in the most relevant comments.

Caveats: vector index isn't supported on Standard editions; autonomous embedding
generation and `AI.SEARCH` are Preview; embeddings + indexes incur storage cost.

## 7. Open table format: Iceberg managed tables

Evolve bronze/silver from native BigQuery tables to **open-format** storage for
multi-engine interoperability (Spark, Flink, and BigQuery on one copy of data):

- Recreate selected layers as **Apache Iceberg managed tables** (data in your own
  Cloud Storage bucket, `file_format = PARQUET`, `table_format = ICEBERG`, `WITH
  CONNECTION`), keeping the same Dataform graph.
- Dataform can create Iceberg tables natively
  (`type: "table"` with the Iceberg config), so the transformation layer is
  unchanged — only the storage format evolves.

Benefits: schema evolution, time travel, automatic storage optimization, and
read access from open-source engines without copying data.

## 8. BI dashboard

Point Looker (or Looker Studio) at the `airport_semantic` views — the demo's
"swap the mini semantic layer for the real one" payoff (see
[`design-philosophy.md`](design-philosophy.md)).

## 9. Data sharing (Analytics Hub)

Publish the curated gold/semantic layer as a governed **data product**:

- Create a **data exchange** and a **listing** over the `airport_gold` /
  `airport_semantic` dataset (private listing for internal teams, or public).
- Subscribers get a read-only **linked dataset** and pay only for their own
  queries; you keep RLS/CLS and **data-egress controls** on the shared data.

This is the "serve beyond a single BI tool" extension — turning the lakehouse
output into a shareable product across org boundaries.

## 10. CI/CD & environments (dev / staging / prod)

Today the pipeline compiles and runs Dataform straight off `main`, triggered
manually — fine for a demo, not for production. Mature it into a managed code
lifecycle:

- Isolate development tables with **workspace compilation overrides** (schema
  suffix), so dev work never touches production tables.
- Drive staging/production with **release configurations** (compile a
  git commitish) + **workflow configurations** (scheduled runs), optionally split
  per Google Cloud project for the strongest isolation.
- Gate promotion on pull requests (`main` → `prod`) and wire Composer/Dataform
  failures into alerting.

A concrete reference model from the Google docs — **split dev → prod by project
+ schema suffix, one Git lineage:**

| Setting | Development | Production |
|---|---|---|
| GCP project | `enterprise-dev` | `enterprise-prod` |
| Git branch | workspace name | `main` |
| Workspace compilation override | schema suffix `${workspaceName}` | none |
| Release config | — | `production` |
| Workflow config (schedule) | — | `production` |

Each developer (or analyst) works in a **personal workspace** that writes to its
own schema suffix (`analytics_sasha`), so no one clobbers anyone; changes go in
via **PR to `main`**; the production release config compiles `main` and the
workflow config schedules it. This is also the answer to the **analyst-pipeline
governance** question (see
[`design-philosophy.md` → Two doors to one engine](design-philosophy.md#part-3--two-doors-to-one-engine-engineer-authored-vs-analyst-authored)):
the governance rule is **one PR-protected repo, analysts get workspaces in it,
every input is a reviewed `declaration`, and we discourage ad-hoc UI-created
"shadow" pipeline repos** — those have separate schedules and logs and never get
promoted. Docs:
[Manage the Dataform code lifecycle](https://docs.cloud.google.com/dataform/docs/managing-code-lifecycle) ·
[Configure compilation](https://docs.cloud.google.com/dataform/docs/configure-compilation) ·
[Schedule runs](https://docs.cloud.google.com/dataform/docs/schedule-runs).

Relates to public-release prep (below): environments need the runtime config
parameterised rather than hardcoded.

## 11. Public-release prep

Today the runtime config (project id, region, connection names, service-account
emails) is hardcoded for the demo project, so the live run stays simple. Before
open-sourcing:

- Parameterise the Composer DAG (`PROJECT_ID`, `REGION`, `REPOSITORY_ID`) via
  Airflow Variables / environment variables instead of module constants.
- Parameterise `workflow_settings.yaml` vars (or override them per-run from the
  DAG's `code_compilation_config.vars`).
- Replace the concrete values in `.env.example` and the doc examples with
  placeholders (`your-project-id`, `YOUR_PROJECT_NUMBER`, …).
- Scrub connection **service-account emails** from `.env.example`; have
  `bootstrap.sh` auto-discover them instead.
- Tighten IAM to least privilege: the execution SA currently gets several
  **project-level** grants that should be scoped down for a shared project:
  - `roles/bigquery.connectionAdmin` (needed for `connections.delegate` when
    creating resources `WITH CONNECTION`) → grant **per-connection** (resource-level
    setIamPolicy on the Spark/Gemini/BigLake connections) instead.
  - `roles/bigquery.dataOwner` + `roles/bigquerydatapolicy.admin` (added for the
    RLS/CLS `security` stage) → scope `dataOwner` to the `airport_governance`
    dataset, and confine data-policy admin to the governance region/policies.
- **Make `teardown.sh` a complete inverse of `bootstrap.sh`.** Today teardown
  reliably deletes the 8 datasets (and everything inside them), the `raw/` data,
  and the Dataform repository — which is enough to enable a clean rebuild (and
  notably drops `airport_ops_control`, so the legacy managed `raw_customer_feedback`
  cannot linger and break the external-table DDL on a fresh run). But it does not
  fully reverse bootstrap:
    - It does **not delete the Dataform service account** (`dataform-airport@…`),
      even though the script header claims it does — fix the code or the comment.
    - It revokes **none of the ~13 IAM bindings** bootstrap adds (execution SA
      project roles, Gemini/Spark connection-SA roles, the Dataform service-agent
      token-creator grant, and the Composer SA's `dataform.admin` +
      `serviceAccountUser`). Either revoke them on teardown or document them as
      intentionally shared (like the connections already are).
    - It clears only `gs://…/raw` data, leaving the **bucket** itself; and it does
      **not remove the uploaded DAG** from the Composer DAG bucket, so the DAG
      stays registered in Airflow.
    - Every destructive step is `… 2>/dev/null && echo ok || echo skip`, which
      **swallows failures** (teardown reports success even if nothing was deleted).
      Surface errors, and add `--force` to `dataform repositories delete` so it
      still succeeds when the repo has workspaces/release configs.

---

Official Google Cloud documentation for each item is linked inline at that item
above.
