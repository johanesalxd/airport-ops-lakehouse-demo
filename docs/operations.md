# Operations runbook

Practical notes for running, monitoring, and troubleshooting the pipeline —
especially **where the logs actually are** (this surprises people coming from
Cloud Composer 2).

## Where logs live

The single most important thing to internalise: **the Airflow task log only tells
you whether a stage ran; the SQL details live in Dataform.** Each `run_<stage>`
task uses `DataformCreateWorkflowInvocationOperator`, which creates a Dataform
workflow invocation and polls it. The actual transformation runs **inside
BigQuery, driven by the Dataform service** — not on the Airflow worker. So the
Airflow log shows only lifecycle lines (worker bootstrap, auth, XCom push, "Task
finished … final_state=success").

| Question | Where to look |
|---|---|
| Did a stage run / what state? | Airflow UI (Grid → task instance), or Cloud Logging |
| **Why did a stage fail (SQL / data error)?** | **BigQuery → Dataform → repo → Workflow Execution Logs** (per-action errors), or the invocation API (below) |
| Query error text / bytes billed | BigQuery → Job history |
| Permission / `actAs` / impersonation errors | Cloud Logging (these *do* surface in the Airflow/worker logs) |

### Airflow task logs are in Cloud Logging (not GCS)

Since Managed Airflow 2.8.0, **Composer 3 stores task logs in Cloud Logging only**
— the environment's `gs://<bucket>/logs/` folder stays empty by default. The
Airflow UI reads logs back from Cloud Logging.

Read a task's log from the CLI:

```bash
gcloud logging read 'resource.type="cloud_composer_environment"
  AND labels."workflow"="airport_ops_lakehouse"
  AND labels."task-id"="run_gold"
  AND labels."run-id"="<RUN_ID>"' --project="$PROJECT_ID" --order=asc --limit=50
```

To also copy logs to the bucket (optional), recreate/update the environment with
`storageMode = CLOUD_LOGGING_AND_CLOUD_STORAGE` (gcloud:
`--disable-logs-in-cloud-logging-only`), or add a Cloud Logging sink.

### Dataform execution logs (the real error source)

Console: **BigQuery → Dataform → `airport-ops-lakehouse-dataform` → Workflow
Execution Logs** → open the invocation → per-action status and error.

Or via the API (what the runbook author used during bring-up):

```bash
TOKEN=$(gcloud auth print-access-token)
BASE="https://dataform.googleapis.com/v1beta1/projects/$PROJECT_ID/locations/$REGION/repositories/$DATAFORM_REPO_ID"
# list recent invocations + state
curl -s -H "Authorization: Bearer $TOKEN" "$BASE/workflowInvocations?pageSize=8"
# per-action results (errors) for one invocation
curl -s -H "Authorization: Bearer $TOKEN" "$BASE/workflowInvocations/<INVOCATION_ID>:query"
```

> For the workshop: show the **Workflow Execution Logs** screen during the live
> run — it visualises the medallion graph executing and is the natural place to
> demonstrate lineage and debugging.

## Composer 3 CLI caveats

Some Airflow CLI commands are **not supported** through
`gcloud composer environments run` on Composer 3:

- `tasks logs` — not supported (use Cloud Logging instead).
- `dags delete-dag-run` — not supported.

Commands that do work and are useful:

```bash
gcloud composer environments run "$COMPOSER_ENV" --location="$REGION" dags list
gcloud composer environments run "$COMPOSER_ENV" --location="$REGION" dags trigger -- -r "<RUN_ID>" airport_ops_lakehouse
gcloud composer environments run "$COMPOSER_ENV" --location="$REGION" tasks states-for-dag-run -- airport_ops_lakehouse "<RUN_ID>"
```

## Idempotency & re-runs

A re-run is always safe — it converges to the same state, no duplicates:

- Native loads use `LOAD DATA OVERWRITE`.
- Spark stored procedures write with `.mode("overwrite")`.
- External tables (BigLake Parquet + the plain feedback JSON-column table), Spark
  procs, and the Gemini model use `CREATE OR REPLACE`.
- Bronze/silver/gold tables are Dataform `type: "table"` → `CREATE OR REPLACE
  TABLE`; semantic views and the `brz_customer_feedback` bronze view are
  `CREATE OR REPLACE VIEW`.
- The synthetic generator is deterministic (fixed seed); upload uses
  `gcloud storage rsync`.

The only per-run change is the `_batch_id` stamp on bronze rows (= the Airflow
`run_id`), for traceability — it replaces, not accumulates. `bootstrap.sh` and the
IAM grants are likewise idempotent.

> Caveat: idempotency holds for a fixed input. Changing `--days`/`--seed`/date
> window generates different data and rsync adds new `dt=` partitions (old ones
> linger in GCS until cleared).

## Known issues & fixes (bring-up history)

These were all resolved during the first live run; kept here as a reference for
anyone reproducing the environment.

| Symptom | Cause | Fix |
|---|---|---|
| `compile_repo` fails: `jinja2 … 'ds_nodash' is undefined` | A `schedule=None` DAG triggered manually on **Airflow 3** has no data interval, so date macros are undefined | Use `{{ run_id }}` for `batchId` |
| Setup actions fail: *does not have permission to generate tokens for `dataform-airport`…* | Dataform **service agent** can't impersonate the execution SA | Grant the Dataform service agent `roles/iam.serviceAccountTokenCreator` on the execution SA (in `bootstrap.sh`) |
| `op_create_spark_procedures` / BigLake fail: *does not have `bigquery.connections.delegate`* | Creating a resource `WITH CONNECTION` needs `delegate`, not just `connections.use` | Grant the execution SA `roles/bigquery.connectionAdmin` (in `bootstrap.sh`) |
| BigLake table fails: *Using multiple asterisks … not supported* | External-table URI had `*/*.parquet` | Single wildcard `*.parquet` (it matches sub-folders) |
| `fct_flight` fails: `Unrecognized name: flight_date` | `partitionBy`/assertions referenced a column that the SELECT had aliased away (`flight_date AS date_key`) | Reference the **output** column name (`date_key`) |
| `wait_<stage>` sensor hangs after a failed stage | Async invoke + state sensor didn't fail fast | Refactored to **synchronous** `DataformCreateWorkflowInvocationOperator` (no sensor) |
