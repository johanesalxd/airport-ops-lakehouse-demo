# Data sharing with BigQuery Analytics Hub (NIO hub-and-spoke)

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
to the curated views, which matches the NIO model (share a governed product; the
subscriber runs normal SQL and pays for it). A **Data Clean Room (DCR)** adds
privacy-preserving analysis rules (aggregation thresholds, join restrictions,
egress blocking) for cases where two parties join sensitive data without either
seeing the other's rows. See the companion
[`data-clean-room-demo`](https://github.com/johanesalxd/data-clean-room-demo)
(`setup_ah_dcr.py`, `E2E_HIERARCHY.md`) for the DCR variant.

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

### Data-owner approval flow (restricted subscriptions)

For an approval step, publish the listing as a **restricted subscription**: the
subscriber's request enters a pending state and the **data owner approves** it
before the linked dataset is created. In the console this is **Analytics Hub →
listing → Subscriptions → Approve/Reject**; programmatically it is the
subscription request/approval on the listing. The whitelisting grant above
controls *who may request*; approval controls *who is admitted*.

## Governance & monitoring

- **Audit logging:** Analytics Hub and BigQuery emit Cloud Audit Logs. Query who
  subscribed and who queried the shared data, e.g.:

  ```sql
  SELECT
    timestamp,
    protopayload_auditlog.authenticationInfo.principalEmail AS principal,
    protopayload_auditlog.methodName AS method,
    resource.labels.project_id AS project_id
  FROM `region-us-central1`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
  -- or the *_cloudaudit_googleapis_com_data_access sink for AH subscribe/list events
  WHERE method LIKE 'google.cloud.bigquery.analyticshub%'
  ORDER BY timestamp DESC
  ```

- **Cost visibility:** subscriber jobs appear in the subscriber project's
  `INFORMATION_SCHEMA.JOBS_BY_PROJECT` with their own `total_bytes_billed`.
- **Fine-grained controls:** RLS/CLS applied upstream (see the `security` stage)
  continue to apply to subscribers querying the shared views.

## Screenshot checklist (for knowledge-sharing)

1. Data Exchange + listing in the publisher's Analytics Hub.
2. Listing IAM showing the whitelisted subscriber principal.
3. (Optional) pending subscription request → approved (data-owner approval).
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
- [Manage subscriptions (approval)](https://docs.cloud.google.com/bigquery/docs/analytics-hub-manage-subscriptions)
- [Authorized datasets](https://docs.cloud.google.com/bigquery/docs/authorized-datasets)
