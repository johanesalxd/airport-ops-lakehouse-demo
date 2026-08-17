# Roadmap

The MVP is a clean, governed batch lakehouse with public clone-and-configure
setup. It already includes optional showcases for fine-grained-access
**governance** (RLS/CLS, §1), **streaming ingestion** (§2), and **automated data
insights** (§4). Those completed showcases stay in this document for context;
the remaining sections are natural next steps kept out of the critical path so
the core demo stays simple.

Each item is grounded in current Google Cloud capabilities, with the relevant
official Google Cloud documentation linked inline.

## Completed showcases

- **Public release prep:** both repos are public-safe. `.env.example` uses
  placeholders, `workflow_settings.yaml` uses placeholder defaults, `bootstrap.sh`
  writes Composer Airflow Variables, and the Composer DAG passes Dataform
  compilation overrides at runtime. Users must explicitly set `SPARK_CONN_SA`,
  `GEMINI_CONN_SA`, and `BIGLAKE_CONN_SA` in `.env`.
- **Governance:** RLS/CLS and masking on a self-contained synthetic staff table.
- **Streaming ingestion:** manual Composer simulator DAG to Pub/Sub, BigQuery
  subscription, bronze stream table, and silver dedupe view.
- **Automated metadata:** optional data-insights script over silver, gold, and
  semantic objects.
- **Data sharing (Analytics Hub):** curated `shr_*` authorized views published as
  a private Data Exchange listing with per-listing subscriber whitelisting and a
  cost-isolated subscriber (spoke) flow. See §9 and
  [`data-sharing.md`](data-sharing.md).

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
> (data policies); group members only need `bigquery.jobUser` (granted by
> `bootstrap.sh`) — their row access comes from the `ROW ACCESS POLICY` grantee
> list and their column access from `GRANT FINE_GRAINED_READ`.
>
> **Do not grant `bigquery.filteredDataViewer` through IAM.** At project or
> dataset level it makes a principal eligible for *every* row access policy in
> scope, so the sales group would inherit `admin_full_access`
> (`FILTER USING (TRUE)`) and see all rows — while CLS masking still behaves
> correctly, which makes the misconfiguration easy to miss. See the
> [row-level security best practices](https://cloud.google.com/bigquery/docs/best-practices-row-level-security).

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

**Implemented** as a hub-and-spoke showcase — see
[`data-sharing.md`](data-sharing.md) for the full runbook.

- A Dataform `share` stage builds curated **`shr_*` authorized views** in
  `airport_share` (over `airport_gold` / `airport_semantic`), so subscribers get
  governed products, not base tables.
- `scripts/setup_analytics_hub.py` (publisher/hub) authorizes the share dataset,
  creates a private **Data Exchange** + **listing**, and whitelists a subscriber
  **on the listing only**.
- `scripts/subscribe_analytics_hub.py` (subscriber/spoke) subscribes, creating a
  read-only **linked dataset** in the subscriber project, and runs a query
  **billed to the subscriber** — demonstrating **cost isolation**.
- `scripts/manage_subscriptions.py` (publisher/hub) **lists and revokes**
  subscriptions — the data-owner governance surface (admission is the subscriber
  grant; revoke detaches a subscriber's linked dataset).
- Governance carries through: upstream RLS/CLS still applies, and Analytics
  Hub/BigQuery audit logs give subscribe/query visibility.

> **Location note:** Analytics Hub resources are created in the shared dataset's
> region (`us-central1`), not the `US` multi-region (`AH_LOCATION` in `.env`).
>
> **Known gap:** `setup_analytics_hub.py` does not set `restricted_export_config`
> on the listing, so **`Data Egress controls` are off** for a script-created
> listing and must be enabled in the console. Wiring it into the script (and
> exposing it as an `.env` toggle) is a small, self-contained follow-up.

Still open as future extensions:

- **Data-egress controls / DCR variant:** privacy-preserving analysis rules
  (aggregation thresholds, join restrictions, restricted export) via a Data Clean
  Room — see the companion
  [`data-clean-room-demo`](https://github.com/johanesalxd/data-clean-room-demo).
- **Commercial / Marketplace listings** with the "Request access → approve"
  flow and monetization — a different listing type from this private-exchange
  showcase (here, admission is the per-listing subscriber grant plus
  list/revoke governance).

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

The public demo already parameterizes runtime config through `.env`, Composer
Airflow Variables, and Dataform compilation overrides. This item takes that one
step further into a production-style promotion model.

## 11. Operational hardening

Public release prep is complete. The remaining work here is operational hygiene
for teams that want to run the demo repeatedly in shared projects or evolve it
toward production practice:

- **Tighten IAM to least privilege:** the execution SA currently gets several
  **project-level** grants that should be scoped down for a shared project:
  - `roles/bigquery.connectionAdmin` (needed for `connections.delegate` when
    creating resources `WITH CONNECTION`) → grant **per-connection** (resource-level
    setIamPolicy on the Spark/Gemini/BigLake connections) instead.
  - `roles/bigquery.dataOwner` + `roles/bigquerydatapolicy.admin` (added for the
    RLS/CLS `security` stage) → scope `dataOwner` to the `airport_governance`
    dataset, and confine data-policy admin to the governance region/policies.
- **Make `teardown.sh` a complete inverse of `bootstrap.sh`.** Today teardown
  deletes the 8 datasets (and everything inside them), the `raw/` data, and
  Pub/Sub streaming resources. It keeps user-managed resources by default. It
  does not fully reverse bootstrap:
    - It does **not delete the Dataform service account** (`dataform-airport@…`).
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
      Surface errors for resources that should be deleted.

---

Official Google Cloud documentation for each item is linked inline at that item
above.
