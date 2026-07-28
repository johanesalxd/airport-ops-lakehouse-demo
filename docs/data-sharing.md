# Data sharing with BigQuery Analytics Hub (hub-and-spoke)

This showcase publishes the curated gold/semantic layer as a governed **data
product** using **BigQuery Analytics Hub**, following a **hub-and-spoke** model:

- The **publisher (hub)** owns the storage and publishes a private **Data
  Exchange** listing over a curated share dataset.
- Each **subscriber (spoke)** links the dataset into its **own** project and pays
  for **its own** query compute — so storage stays with the publisher and cost is
  isolated per subscriber.

It is the "serve beyond a single BI tool" extension: the lakehouse output becomes
a shareable product across organizational boundaries, while the publisher keeps
governance (curated views, authorized datasets, per-listing whitelisting).

```
Publisher HUB (owns storage)                Subscriber SPOKE (pays compute)
  airport_gold ─┐                             linked dataset (read-only)
  airport_semantic ─┤ authorized dataset          │
                    ▼                              ▼
             airport_share (shr_* views)     queries billed to the
                    │                          subscriber project
                    ▼  Analytics Hub
             private Data Exchange ── listing ──► subscribe ──► linked dataset
                    │
                    └─ subscriber whitelisted on the listing (roles/analyticshub.subscriber)
```

## Why a curated share dataset (not raw gold)

Subscribers should not see base tables. The Dataform `share` stage builds
**`shr_*` views** in `airport_share`:

| View | Source | Product |
|---|---|---|
| `shr_airport_operations_daily` | `sem_airport_operations_daily` | daily ops KPIs |
| `shr_flight_performance` | `fct_flight` | flight performance by date/terminal/airline |

`airport_share` is added as an **authorized dataset** on `airport_gold` and
`airport_semantic`, so the `shr_*` views resolve for subscribers without exposing
the underlying tables. `scripts/setup_analytics_hub.py` performs this
authorization automatically before publishing.

## DCX vs DCR

This showcase uses a standard **Data Exchange (DCX)** — direct, read-only access
to the curated views (share a governed product; the subscriber runs normal SQL
and pays for it). A **Data Clean Room (DCR)** adds privacy-preserving analysis
rules (aggregation thresholds, join restrictions, egress blocking) for cases
where two parties join sensitive data without either seeing the other's rows. See
the companion
[`data-clean-room-demo`](https://github.com/johanesalxd/data-clean-room-demo) for
the DCR variant.

## Prerequisites

- The lakehouse is deployed and the `share` stage has run (so `airport_share`
  contains the `shr_*` views). Trigger the `airport_ops_lakehouse` DAG, which now
  runs `… → security → share`.
- **Location:** Analytics Hub resources must be created in the **same region as
  the shared datasets** (here `us-central1`), not the `US` multi-region. This is
  set by `AH_LOCATION` in `.env`.
- Config in `.env` (see `.env.example`): `DS_SHARE`, `AH_LOCATION`,
  `ANALYTICS_HUB_EXCHANGE`, `ANALYTICS_HUB_LISTING`, `SUBSCRIBER_PROJECT`,
  `SUBSCRIBER_PRINCIPAL`, `SUBSCRIBER_LINKED_DATASET`.
- `bigqueryanalyticshub.googleapis.com` enabled in both projects.

### IAM

**Publisher side** (run as an identity with `roles/analyticshub.admin` +
`roles/bigquery.dataOwner` on the datasets):

- The setup script grants `roles/analyticshub.subscriber` to
  `SUBSCRIBER_PRINCIPAL` **on the listing only**.

> ⚠️ **Isolation pitfall:** never grant `roles/analyticshub.subscriber` at the
> project level — that lets a consumer subscribe to *every* listing in the
> project. Grant it per-listing (as the script does).

**Subscriber side** (in `SUBSCRIBER_PROJECT`):

- `roles/analyticshub.subscriptionOwner` — to create the subscription.
- `roles/bigquery.user` — to create the linked dataset and run queries
  (`roles/bigquery.jobUser` alone is not enough).

## Publisher workflow (hub)

```bash
source .env
bash scripts/setup_analytics_hub.sh
```

This: (1) authorizes `airport_share` on `airport_gold` + `airport_semantic`,
(2) creates the private Data Exchange, (3) creates a listing over
`airport_share`, and (4) whitelists `SUBSCRIBER_PRINCIPAL` on the listing.

## Subscriber onboarding (spoke) + cost isolation

```bash
source .env
bash scripts/subscribe_analytics_hub.sh
```

This subscribes from `SUBSCRIBER_PROJECT`, creating the read-only linked dataset
`SUBSCRIBER_LINKED_DATASET`, then runs a sample query **billed to the subscriber
project** and prints the job's project + bytes billed — demonstrating cost
isolation. Use `--skip-query` to only subscribe.

### Data-owner approval & governance

A **private** listing has no separate pending/approve queue — that "Request
access → approve" flow is a **commercial/Marketplace** listing feature. For a
private exchange, the data owner governs access in three steps:

1. **Admission** — granting `roles/analyticshub.subscriber` on the listing (done
   by `setup_analytics_hub.sh`). *This grant is the approval decision:* only
   whitelisted principals can subscribe.
2. **Visibility** — list who has subscribed:

   ```bash
   source .env && bash scripts/manage_subscriptions.sh --list
   ```

3. **Revocation** — revoke a subscription (detaches the subscriber's linked
   dataset):

   ```bash
   source .env && bash scripts/manage_subscriptions.sh \
     --revoke projects/SUBSCRIBER_NUMBER/locations/us-central1/subscriptions/SUB_ID
   ```

   (Use the subscription name printed by `--list`.)

In the console the same surface is **Analytics Hub → listing → set permissions**
(admission) and **→ listing → Subscriptions** (view/revoke).

## Governance & monitoring

**Cost visibility (cost isolation) — run in the SUBSCRIBER project.** Subscriber
queries against the linked dataset are billed to the subscriber, visible in its
own `INFORMATION_SCHEMA.JOBS_BY_PROJECT`:

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

**Audit logging.** Analytics Hub emits Cloud Audit Logs under the service
`analyticshub.googleapis.com`. Two things determine *where* an event lands (see
[Sharing audit logging](https://docs.cloud.google.com/bigquery/docs/analytics-hub-audit-logging)):

- **Log type.** Write methods (`CreateDataExchange`, `CreateListing`,
  `SetIamPolicy` = whitelisting, `DeleteListing`, `RevokeSubscription`) are
  **Admin Activity** (always on). Read methods (`ListSharedResourceSubscriptions`,
  `GetListing`) are **Data Access**, which is **off by default** — enable it first
  ([how](https://docs.cloud.google.com/logging/docs/audit/configure-data-access)).
- **Project.** Publisher events (publish/whitelist/revoke) are logged in the
  **publisher** project. `SubscribeListing` is logged in the **subscriber**
  project (that is where the call originates). For "who subscribed", the simplest
  view is `manage_subscriptions.sh --list`.

To query events in BigQuery, route them with a
[log sink](https://docs.cloud.google.com/logging/docs/export/configure_export_v2)
(one-time setup, publisher project):

```bash
# 1. Enable Data Access audit logs for analyticshub.googleapis.com (console:
#    IAM & Admin -> Audit Logs), or via the project IAM policy auditConfigs.
# 2. Create a dataset and a partitioned-table sink filtered to the service:
bq --location=us-central1 mk --dataset "${PROJECT_ID}:sharing_audit"
gcloud logging sinks create sharing_audit_sink \
  "bigquery.googleapis.com/projects/${PROJECT_ID}/datasets/sharing_audit" \
  --use-partitioned-tables \
  --log-filter='protoPayload.serviceName="analyticshub.googleapis.com"'
# 3. Grant the printed writer identity roles/bigquery.dataEditor on the dataset.
```

> New sink tables take a few minutes to appear on first write (Logging streams in
> small batches).

Then query the publisher governance events (publish, whitelist, revoke):

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

For reads (e.g. who listed subscriptions), swap the table for
`cloudaudit_googleapis_com_data_access` (requires Data Access logs enabled).

- **Usage metrics:** the publisher's listing has a built-in
  [usage-metrics dashboard](https://docs.cloud.google.com/bigquery/docs/analytics-hub-monitor-listings)
  (consumption by subscriber, data volume) — no query needed.
- **Fine-grained controls:** RLS/CLS applied upstream (see the `security` stage)
  continue to apply to subscribers querying the shared views.

## Screenshot checklist (for knowledge-sharing)

1. Data Exchange + listing in the publisher's Analytics Hub.
2. Listing permissions showing the whitelisted subscriber principal (admission).
3. Publisher's **Subscriptions** view for the listing (who subscribed) —
   `manage_subscriptions.sh --list` or the console.
4. Subscriber's Explorer showing the linked dataset (read-only).
5. Sample query result + the job details showing it ran/billed in the subscriber
   project (cost isolation).
6. Audit-log query results.

## Teardown

`scripts/teardown.sh` removes the subscriber linked dataset, the listing, the
data exchange, and the `airport_share` dataset (in addition to the base demo
resources).

## Google Cloud references

- [Introduction to Analytics Hub](https://docs.cloud.google.com/bigquery/docs/analytics-hub-introduction)
- [Create and manage a data exchange](https://docs.cloud.google.com/bigquery/docs/analytics-hub-manage-exchanges)
- [Create and manage listings](https://docs.cloud.google.com/bigquery/docs/analytics-hub-manage-listings)
- [Subscribe to a listing](https://docs.cloud.google.com/bigquery/docs/analytics-hub-view-subscribe-listings)
- [Manage subscriptions (view/revoke)](https://docs.cloud.google.com/bigquery/docs/analytics-hub-manage-subscriptions)
- [Sharing audit logging](https://docs.cloud.google.com/bigquery/docs/analytics-hub-audit-logging)
- [Monitor listings (usage metrics)](https://docs.cloud.google.com/bigquery/docs/analytics-hub-monitor-listings)
- [Authorized datasets](https://docs.cloud.google.com/bigquery/docs/authorized-datasets)
