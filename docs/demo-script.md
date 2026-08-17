# Demo script — Airport Operations Lakehouse (15–25 min)

A "concept then live" runbook for the workshop. Use the project and region from
your `.env` file.

## Assumed one-time platform setup

These exist already and are *reused* by the demo (not created by the scripts): the
three BigQuery connections (Spark, Gemini, BigLake), the Cloud Composer
environment, the GCP Dataform repository linked to the companion Git repo, the
Secret Manager secret holding the Git token, and the **two Google Groups** for the
RLS/CLS stage (`bq-rls-cls-dataform-admin@…`, `bq-rls-cls-dataform-sales@…` —
`bootstrap.sh` grants them `bigquery.jobUser` only, not a read role, but cannot
create groups). See
[`architecture.md`](architecture.md) for how they are wired.

## Pre-flight checklist (10 min before the session)

- **Two browser profiles / windows signed in to BigQuery:**
  - **A — admin:** your normal identity, a member of `bq-rls-cls-dataform-admin@…`
    (sees all rows + raw values in §7c).
  - **B — sales:** a member of `bq-rls-cls-dataform-sales@…` (sees filtered rows +
    masked columns in §7c). Needed for the RLS/CLS reveal.
- **Tabs to pre-open:** Composer Airflow UI (`dev-airflow` → DAG
  `airport_ops_lakehouse`), BigQuery Studio, the BigQuery **Dataform** repo page,
  the **Sharing (Analytics Hub)** page (exchange → listing), and the slide deck
  (PDF).
- **A third window signed in as the subscriber**, with the console project set to
  `SUBSCRIBER_PROJECT` — needed for §7d to show the linked dataset and the
  cost-isolated query landing in the *subscriber's* job history.
- **Project context:** console / `bq` set to your `PROJECT_ID`. Paste the §6–§7d
  queries into a scratch BigQuery tab ahead of time (see
  `scratch/demo-queries.sql`, which has the project IDs already substituted).
- **Confirm the latest run is green:** open the most recent
  `airport_ops_lakehouse` run grid — all 11 tasks green. You will **reuse** this
  run (not re-trigger) so the built tables are already populated.
- **Sanity check** one query returns rows (e.g. `sem_airport_operations_daily`)
  and one *subscriber-side* query returns rows from the linked dataset.
- **Do NOT run `scripts/teardown.sh`** between rehearsal and the live session — it
  deletes the audit-log sink and dataset that §7d step 5 depends on, and new sink
  tables take minutes to reappear.

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
  semantic → quality → security → share → publish_run_summary`. Only **trigger** a fresh
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
-- bq-rls-cls-dataform-admin@  -> 6 rows, real ssn/email/salary
-- bq-rls-cls-dataform-sales@  -> 2 rows (Sales only), email "" / salary NULL /
--                                ssn SHA256
SELECT staff_id, name, email, department, salary, ssn
FROM `your-project-id.airport_governance.staff_directory`
ORDER BY staff_id;
```

> **Use the explicit column list above — do not run `SELECT *`.** `bank_account`
> carries a `RAW_DATA_ACCESS_POLICY` with no masking policy to fall back to, so
> for the sales identity *any* query touching that column fails outright rather
> than masking it. `SELECT *` therefore returns a permission error for sales and
> the row-filtering comparison is lost.

Then show that denial **deliberately**, as the closing beat — same table, one
extra column:

```sql
-- bq-rls-cls-dataform-admin@  -> 6 rows, real bank_account values
-- bq-rls-cls-dataform-sales@  -> Access Denied: "does not have masked access or
--                                raw data access to protected columns:
--                                ...staff_directory.bank_account"
SELECT staff_id, department, bank_account
FROM `your-project-id.airport_governance.staff_directory`
ORDER BY staff_id;
```

Say: *"Same query, same table — the result depends on who's asking. Rows are
filtered by RLS; sensitive columns are masked, hashed, or blocked entirely by CLS
data policies. Four different behaviours on one table, no views, no copies, all
declared in Dataform alongside the transformations."*

> If the sales view still shows all rows *and* raw values, the data-policy IAM
> may need a minute to propagate after the run — re-run the query.
>
> If the sales view shows **all rows but correctly masked columns**, that is
> **not** propagation. It means a principal holds
> `roles/bigquery.filteredDataViewer` directly through IAM, which makes it
> eligible for every row access policy in scope — including
> `admin_full_access` (`FILTER USING (TRUE)`). Check with:
>
> ```bash
> gcloud projects get-iam-policy your-project-id \
>   --flatten="bindings[].members" \
>   --format="value(bindings.members,bindings.role)" \
>   | grep filteredDataViewer
> ```
>
> The fix is to remove that binding: group members need only
> `bigquery.jobUser`, because the `ROW ACCESS POLICY` grantee list already
> grants row access.

## 7d. Data sharing: Analytics Hub hub-and-spoke (45–60 min) — live

The whole model in one line: **the hub owns storage, the spokes pay compute.**
Full runbook: [`data-sharing.md`](data-sharing.md).

This section is structured in four parts. Each stands alone, so you can go deep
on whichever the audience cares about and skim the rest.

### 7d.1 Architecture alignment (10 min)

Draw or show the hub-and-spoke picture before touching a console:

```
Publisher HUB (owns storage)            Subscriber SPOKE (pays compute)
  airport_gold ─┐                         linked dataset (read-only)
  airport_semantic ─┤ authorized dataset      │
                    ▼                          ▼
             airport_share (shr_* views)  queries billed to the
                    │                       subscriber project
                    ▼  Analytics Hub
             private Data Exchange ── listing ──► subscribe ──► linked dataset
```

Points to land:

- The `share` stage built curated `shr_*` views in `airport_share` over
  gold/semantic. Subscribers get **products, not base tables**.
- `airport_share` is an **authorized dataset** on `airport_gold` and
  `airport_semantic`, so the views resolve for subscribers *without* granting
  them any access to the underlying tables. Show this in the dataset's
  **Sharing → Authorized datasets** panel.
- **Zero copy.** The subscriber's linked dataset is a pointer. Storage is billed
  once, to the publisher; nothing is replicated or exported.
- **One publisher project, N subscriber projects.** Adding the second, third, and
  tenth spoke is the same two commands — this is the part that scales.

> **Simplification to call out:** this demo puts the exchange in the *same*
> project as the storage. At scale, separate them — a storage project holding the
> curated datasets and a governance project holding the exchange and listings.
> That keeps entitlement administration away from data administration.

### 7d.2 Publisher experience — the admin workflow (10 min)

**Lead in the console.** This is the part the audience will screenshot, and a
terminal is worthless to a non-engineer. The setup already exists, so walk the
result and show the *form* rather than re-creating anything.

In **`Sharing (Analytics Hub)`**:

1. The **data exchange** — note the `Private` discovery type.
2. Click **`Create exchange`** to show the form: `Project` / `Region`
   (immutable), `Display name`, `Primary contact`, and the
   **`Subscriber Email Logging`** and **`Public Discoverability`** toggles. Then
   **cancel** — the point is the shape of the decision, not another exchange.
   - On email logging, say it out loud: it logs *which individual* at the partner
     ran each query (`INFORMATION_SCHEMA.SHARED_DATASET_USAGE.job_principal_subject`),
     and it is a **one-way door** — to turn it off you delete and recreate the
     exchange.
3. The **listing** — the icon, description, categories, and the rendered
   **`Documentation`** Markdown. This is the data contract a subscriber reads.
4. **`Create listing`** → **`Configure data`** → **`Data Egress controls`**. Show
   that `Disable copy and export of shared data` is on. This matters later: it is
   one of the controls the publisher *keeps* after handing over compute.
5. Listing → **`Set permissions`** → the principal holding
   **`Analytics Hub Subscriber`**, scoped to the listing.

> ⚠️ **The isolation pitfall worth stating out loud:** never grant
> `roles/analyticshub.subscriber` at the *project* level. That lets a consumer
> subscribe to *every* listing in the project. Grant it **per listing**. This is
> the single most common mistake in a hub-and-spoke rollout.

**Then reveal the script** — this is the stronger ordering:

```bash
source .env && bash scripts/setup_analytics_hub.sh
```

Idempotent, so it is safe to run live; it will report `already exists` and move
on. Frame it as: *"everything you just watched me click is four API calls. This is
how you onboard partner number ten, not partner number one."*

Console click-paths and the matching Google Cloud documentation links are in
[`data-sharing.md`](data-sharing.md#doing-the-same-thing-in-the-console) — useful
if someone asks "how would we reproduce this ourselves?"

### 7d.3 Subscriber onboarding & cost isolation (10 min)

Switch to the **subscriber window**. Again, console first.

**Sign in as the subscriber, not as yourself.** This section is worth far more
performed by a second identity that holds only `Analytics Hub Subscriber` on the
listing and rights in its *own* project. Everything below then happens as the
partner, and the cost evidence at the end carries their email rather than yours.
If you run it as the publisher, say so — an audience that spots one identity
doing both halves will discount the isolation claim.

**How a subscriber finds a private listing.** In the subscriber's
`Sharing (Analytics Hub)` → **`Search listings`** → Filters → `Listings` →
**`Private`**. Worth stating: private listings are not broadly browsable — the
publisher hands over the **listing URL**. Making them discoverable in the catalog
means making the exchange public, which is usually not what you want for partner
data.

**Subscribing.** Click the listing → **`Subscribe`** → the
**`Create linked dataset`** dialog asks for `Project` and
`Linked dataset name` → **`Save`**.

> Check the `Project` field before saving. It defaults to whatever project the
> console is currently in, and landing the linked dataset in the publisher's
> project quietly destroys the entire point of this section.

The equivalent, for the tenth partner:

```bash
source .env && bash scripts/subscribe_analytics_hub.sh
```

Then show, in the subscriber's console:

1. The **linked dataset** in **`Explorer`** — point out it has a **different
   icon** from a normal dataset. Read-only, no copy, it is a pointer.
2. A query against `shr_airport_operations_daily` returning rows.
3. **The punchline — where the cost landed.** Run this *in the subscriber
   project*; it proves compute was billed to the spoke, not the hub:

   ```sql
   SELECT
     job_id,
     user_email,
     total_bytes_billed,
     creation_time
   FROM `region-us-central1`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
   WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
     AND state = 'DONE'
   ORDER BY creation_time DESC;
   ```

   Two columns carry the argument. `user_email` is the subscriber's identity, and
   the rows exist **in the subscriber's project history at all** — the publisher's
   `JOBS_BY_PROJECT` has no record of this query. Say it plainly: *"the hub never
   saw this job, and the hub is not paying for it."*

> Do not lean on the `total_bytes_billed` printed by the subscribe script — these
> views are tiny, so it will often read `0` (under the 10 MB minimum, or cached).
> `JOBS_BY_PROJECT` in the subscriber project is the artifact that actually makes
> the point: the job **exists in their project's history, not yours**.

### 7d.4 Governance, approval & monitoring (15 min)

**Who has access, and how do you take it away.** In the console: listing →
**`Manage subscriptions`**. You will see one `STATE_ACTIVE` subscription and one
`STATE_INACTIVE` — the inactive one was revoked earlier. Access is gone, the
record is retained. That is the revocation workflow, visible.

To show the revoke path itself: **`Subscriptions`** → tick a subscription →
**`Remove Subscriptions`** → the **`Remove subscription?`** dialog requires you
to type `remove` → **`Remove`**. Show the dialog; you do not have to go through
with it on the live subscription.

Scripted equivalent:

```bash
source .env && bash scripts/manage_subscriptions.sh --list
```

**The admission gate, demonstrated.** A second identity holds
**`Analytics Hub Viewer`** on the exchange but *not*
`Analytics Hub Subscriber` on the listing. Signed in as that identity, the
listing is **visible but cannot be subscribed to** — attempting it returns
`PERMISSION_DENIED` on `analyticshub.listings.subscribe`. Grant them
`Analytics Hub Subscriber` via **`Set permissions`**, refresh, and the path
opens.

That round trip *is* the approval: viewer sees it, cannot act, owner grants,
consumer proceeds. The listing's **`Request access contact`** is the channel they
use to ask.

**Approval models.** Expect the question *"where is the UI for the data owner to
approve a request?"* The honest answer has three parts — see the table in
[`data-sharing.md`](data-sharing.md#which-approval-model-do-you-actually-get):

| Model | Consumer sees | Owner's approval action |
|---|---|---|
| Private listing (this demo) | **Subscribe** | Grant `analyticshub.subscriber` on the listing |
| Request access | **Request access** form → `primaryContact` | Grant `analyticshub.subscriber` in response |
| Marketplace-integrated | **Purchase via Marketplace** | Automatic on order activation |

The framing: **there is no in-console pending-approval queue with an Approve
button.** In the first two models the IAM grant *is* the approval. If you need a
real workflow — ticket, reviewer, SLA — build it in front of the grant; the
scripts here show the exact API surface (`setIamPolicy`, `listSubscriptions`,
`revokeSubscription`) that such a portal would call. And you are not locked in:
requesting-access and Marketplace flows are supported on the *same* listing, so a
private listing can gain a commercial flow later without disrupting existing
subscriptions.

**Audit logging.** Analytics Hub emits Cloud Audit Logs under
`analyticshub.googleapis.com`, routed here to BigQuery via a log sink:

```sql
SELECT
  timestamp,
  protopayload_auditlog.authenticationInfo.principalEmail AS actor,
  protopayload_auditlog.methodName AS method,
  protopayload_auditlog.resourceName AS resource
FROM `PROJECT.sharing_audit.cloudaudit_googleapis_com_activity`
WHERE protopayload_auditlog.serviceName = 'analyticshub.googleapis.com'
ORDER BY timestamp DESC;
```

Point out: publish/whitelist/revoke are **Admin Activity** (always on) and land in
the *publisher* project; `SubscribeListing` is logged in the *subscriber* project;
read methods are **Data Access** and are off by default.

**Usage metrics — no query needed.** Exchange → **`Usage metrics`** tab → pick
the listing and a time range. Shows Total Subscriptions, Total Subscribers, Total
jobs executed, Total bytes scanned, a Daily Subscriptions chart,
**Subscribers per organization**, Daily Executed Jobs, and Tables' job frequency.
`Subscribers per organization` is the one to linger on if the audience cares
about multi-party sharing.

**Also mention:** upstream **RLS/CLS from the `security` stage still applies** to
subscribers querying the shared views — fine-grained control survives the sharing
boundary.

**What the publisher keeps vs. gives up.** Worth being explicit, because it is
the question behind most governance concerns:

| Publisher keeps | Publisher gives up |
|---|---|
| Who may subscribe (per-listing IAM) | Query cost — billed to the subscriber |
| What is exposed (curated views) | Slot/spend limits inside the subscriber's project |
| Row/column visibility (RLS/CLS) | When and how often they query |
| Copy/export restrictions (`Data Egress controls`) | |
| Revocation, instantly | |
| Visibility (usage metrics + audit logs) | |

If someone expects the publisher to cap a subscriber's compute: that is not
possible once compute is delegated, and it is the direct trade for
"subscriber-paid compute". The controls in the left column are the answer.

### Closing line for this section

Say: *"The hub publishes once; each subscriber subscribes into its own project and
pays for its own queries. You keep the data and the governance; they get a live,
read-only product — no copies, no egress of raw rows. Adding the next partner is
the same two commands."*

## 8. Close (2 min) — concept

- Transformation (Dataform) vs semantics (views/Looker) — clean separation.
- Spark for messy ingestion, called from Dataform, orchestrated by Composer.
- Governance built in: RLS + CLS/masking (the `security` stage), plus Gemini
  auto-metadata. Optional Pub/Sub baggage streaming is available as a separate
  manual DAG; continuous queries remain the next real-time extension.
- Data sharing beyond BI: Analytics Hub hub-and-spoke (the `share` stage +
  setup/subscribe scripts) — cost isolation per subscriber.

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
