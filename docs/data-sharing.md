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
  `ANALYTICS_HUB_EXCHANGE`, `ANALYTICS_HUB_LISTING`, `AH_PRIMARY_CONTACT`,
  `SUBSCRIBER_PROJECT`, `SUBSCRIBER_PRINCIPAL`, `SUBSCRIBER_LINKED_DATASET`.
  `AH_PRIMARY_CONTACT` is shown to subscribers in the console as the data owner
  to contact, and is where "Request access" submissions land — use a monitored
  group, not an individual.
- `analyticshub.googleapis.com` enabled in both projects.

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

## Doing the same thing in the console

The scripts exist so this is repeatable for the tenth partner. Everything they do
is also available in the Google Cloud console, which is usually what you want
when walking someone through the model for the first time.

The console page is **`Sharing (Analytics Hub)`**. Product docs now call the
service "BigQuery sharing (formerly Analytics Hub)"; the console label still
carries both names.

### Publisher — create the exchange

`Sharing (Analytics Hub)` → **`Create exchange`**

| Field | Notes |
|---|---|
| `Project`, `Region` | **Immutable** after creation. Region must match the shared dataset. |
| `Display name` | The only required field. |
| `Primary contact` | Email **or** URL. |
| `Description` | Free text. |
| `Subscriber Email Logging` (toggle) | Logs *which individual* ran each query, surfaced in `INFORMATION_SCHEMA.SHARED_DATASET_USAGE.job_principal_subject`. |
| `Public Discoverability` (toggle) | Leave off for a private exchange. |

Confirm with **`Create Exchange`**, then either fill the inline
`Exchange Permissions` block (`Administrators` / `Publishers` / `Subscribers` /
`Viewers`) and click **`Set permissions`**, or click **`Skip`**.

> ⚠️ **`Subscriber Email Logging` is a one-way door.** Once enabled and saved it
> cannot be edited — the only way to turn it off is to delete the data exchange
> and recreate it. Decide before you save.

The toggle is narrower than its name suggests, and the distinction is worth
knowing before you agonise over it. With it **off**, the publisher still sees
**who subscribed** — `Manage subscriptions` reports each subscription's
`subscriberContact`. What is withheld is **who ran each individual query**.

So off means: *"I know which parties hold access; I do not track which of their
employees queried what."* For most partner-sharing arrangements that is the
defensible position, and it is usually the one a privacy or legal reviewer will
prefer. Turn it on only when per-user attribution is a stated requirement.

### Publisher — create the listing

Exchange → **`Create listing`**

1. **`Configure data`** — `Resource type` (`BigQuery dataset` / `Pub/Sub Topic`),
   then `Shared dataset` (**immutable** after creation),
   `Allow stored procedure sharing` (Preview), `Region data availability`
   (regions show as `Ready to use`, `Unavailable`, or `Provider primary`), and
   **`Data Egress controls`**:
   - `Disable copy and export of shared data`
   - `Disable copy and export of query results` (also selects the first)
   - `Disable copy and export of tables through APIs` (also selects the first)

   > **Console-only in this repo.** `scripts/setup_analytics_hub.py` does not set
   > `restricted_export_config`, so a listing created by the script has egress
   > controls **off**. Enable them in the console afterwards if you need them.
   > Tracked as a follow-up in [`roadmap.md`](roadmap.md).
2. **`Listing details`** — `Display name` (required), plus optional `Category`
   (up to two), `Data affinity`, `Icon` (PNG/JPEG, <512 KiB, ≤512×512),
   `Description`, `Public discoverability`, `Subscriber Email Logging`, and
   **`Documentation > Markdown`**.
3. **`Listing contact information`** — `Primary contact`,
   **`Request access contact`**, `Provider` (`Provider name`,
   `Provider primary contact`), `Publisher` (`Publisher name`,
   `Publisher primary contact`). All optional.
4. Review **`Listing preview`**, then **`Publish`**.

> `Documentation > Markdown` is worth filling in properly — it renders on the
> listing page and is the closest thing to a published data contract. A listing
> with a column dictionary and a refresh statement reads like a product; one
> without reads like a leftover dataset.

**Alternate entry point:** BigQuery → click a dataset → **`Sharing`** >
**`Publish as listing`**. Useful when the audience thinks dataset-first.

### Publisher — whitelist a partner

Exchange → listing → **`Set permissions`** → **`Add principal`** →
`New principals` → `Select a role` → point to **`Analytics Hub`** → choose:

| Role in the UI | Effect |
|---|---|
| **`Analytics Hub Subscriber`** | Can subscribe. Use for a private listing. |
| **`Analytics Hub Viewer`** | Can *see* the listing but not subscribe — this is what drives the `Request access` path. |

Then **`Save`**.

Private listings are not browsable by default. To hand one to a partner, send
them the **listing URL**; making it discoverable instead requires making the data
exchange public.

### Subscriber — discover and subscribe

`Sharing (Analytics Hub)` → **`Search listings`** → Filters → `Listings` →
**`Private`** (you can also filter by `Categories`, `Location`, `Provider`) →
click the listing → **`Subscribe`** → the **`Create linked dataset`** dialog asks
for `Project` and `Linked dataset name` → **`Save`**.

The linked dataset then appears in the subscriber's BigQuery **`Explorer`** pane
**with a different icon** from a normal dataset — a small but useful visual cue
that it is a pointer, not a copy.

### Publisher — subscriptions and revocation

Exchange → listing → **`Manage subscriptions`** (also reachable from BigQuery →
shared dataset → **`Sharing`** > `Manage subscriptions`). Results can be filtered
by subscriber.

To revoke: **`Subscriptions`** → tick the subscriptions → **`Remove Subscriptions`**
→ in the **`Remove subscription?`** dialog type `remove` → **`Remove`**.

A revoked subscription remains listed as `STATE_INACTIVE`. Access is gone; the
record is kept.

### Publisher — usage metrics

Exchange → **`Usage metrics`** tab → select a listing and a time range. Shows
Total Subscriptions, Total Subscribers, Total jobs executed, Total bytes scanned,
a Daily Subscriptions chart, **Subscribers per organization**, Daily Executed
Jobs, and Tables' job frequency. No query required.

### Console-only affordances worth knowing

- **`Listing preview`** — renders the listing card as you fill the form. No API
  equivalent.
- **`Copy share link`** (exchange, under **`More options`**) — the documented
  workaround when a publisher is in a *different organization* and therefore
  cannot see your exchange.
- **`Copy public link`** (listing) — unauthenticated URL, for public listings
  granted `allUsers` the Viewer role.
- **`Publish as listing`** from a dataset.
- Bulk subscription removal via checkboxes.

And the inverse — **API-only**, not exposed in the console: `icon` and
`documentation` on a **data exchange** can be set via
`dataExchanges.patch`, but the `Create exchange` / `Edit exchange` dialogs only
offer `Display name`, `Primary contact`, `Description`, `Public discoverability`
and `Subscriber Email Logging`.

### Console click-path reference

| Task | Documentation |
|---|---|
| Create a data exchange | [manage-exchanges#create-exchange](https://docs.cloud.google.com/bigquery/docs/analytics-hub-manage-exchanges#create-exchange) |
| Make an exchange public | [manage-exchanges#make-data-exchange-public](https://docs.cloud.google.com/bigquery/docs/analytics-hub-manage-exchanges#make-data-exchange-public) |
| Update an exchange | [manage-exchanges#update-exchange](https://docs.cloud.google.com/bigquery/docs/analytics-hub-manage-exchanges#update-exchange) |
| Create a listing | [manage-listings#create_a_listing](https://docs.cloud.google.com/bigquery/docs/analytics-hub-manage-listings#create_a_listing) |
| Give users access to a listing | [manage-listings#give_users_access_to_a_listing](https://docs.cloud.google.com/bigquery/docs/analytics-hub-manage-listings#give_users_access_to_a_listing) |
| Delegate listing administration | [manage-listings#create-listing-administrator](https://docs.cloud.google.com/bigquery/docs/analytics-hub-manage-listings#create-listing-administrator) |
| View all subscriptions | [manage-listings#view_all_subscriptions](https://docs.cloud.google.com/bigquery/docs/analytics-hub-manage-listings#view_all_subscriptions) |
| Remove a subscription | [manage-listings#remove_a_subscription](https://docs.cloud.google.com/bigquery/docs/analytics-hub-manage-listings#remove_a_subscription) |
| Discover and subscribe | [view-subscribe-listings](https://docs.cloud.google.com/bigquery/docs/analytics-hub-view-subscribe-listings) |
| Usage metrics | [monitor-listings](https://docs.cloud.google.com/bigquery/docs/analytics-hub-monitor-listings) |
| Commercial / Marketplace | [cloud-marketplace](https://docs.cloud.google.com/bigquery/docs/analytics-hub-cloud-marketplace) |
| Audit logging | [audit-logging](https://docs.cloud.google.com/bigquery/docs/analytics-hub-audit-logging) |

### Data-owner approval & governance

For a **private** listing, the data owner governs access in three steps:

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

   (Use the subscription name printed by `--list`.) A revoked subscription stays
   listed with `STATE_INACTIVE` — the audit trail is retained, the access is not.

In the console the same surface is **Analytics Hub → listing → set permissions**
(admission) and **→ listing → Subscriptions** (view/revoke).

#### Which approval model do you actually get?

"Approval" means different things depending on the listing type. There are three
distinct models, and they can coexist on the same listing.

| Model | Consumer experience | Owner's approval action | When to use |
|---|---|---|---|
| **Private listing** (this showcase) | Owner sends them the listing URL; **Subscribe** is available immediately | Grant `roles/analyticshub.subscriber` on the listing, out of band | Known partners, contracts already in place |
| **Request access** | Consumer can *see* the listing but not subscribe, so the console shows **Request access** and a request form that goes to the listing's `primaryContact` | Grant `roles/analyticshub.subscriber` in response to the request | Self-service discovery, owner still gates every grant |
| **Marketplace-integrated** | **Purchase via Marketplace** → order → access granted on order activation | Onboard the product once in the Producer Portal; per-order approval is automatic | Commercial data products, entitlement tied to billing |

Three things follow from this that are easy to get wrong:

- **There is no in-console "pending approvals" queue with an Approve button.** In
  the first two models the IAM grant *is* the approval; rejection is simply not
  granting. Only the Marketplace model has an order lifecycle.
- **"Request access" needs the consumer to be able to see the listing.** Grant
  `roles/analyticshub.viewer` (on the exchange) for that, and set a monitored
  `AH_PRIMARY_CONTACT` — that address receives the request. Making a listing
  broadly discoverable otherwise requires
  [making the data exchange public](https://docs.cloud.google.com/bigquery/docs/analytics-hub-manage-exchanges#make-data-exchange-public).
- **You are not locked in.** Per the
  [Marketplace docs](https://docs.cloud.google.com/bigquery/docs/analytics-hub-cloud-marketplace),
  requesting-access and Marketplace flows are *both* supported on a single
  listing, and you can add Marketplace integration to an existing listing
  "without any disruptions to existing subscriptions." Start private, add
  commercial later.

If you need a genuine approval **workflow** — ticket, reviewer, SLA, audit of the
decision itself — build it in front of the IAM grant. The
`manage_subscriptions.py` script shows the exact API surface such a portal calls
(`listSubscriptions`, `revokeSubscription`); admission is a `setIamPolicy` on the
listing. Analytics Hub deliberately owns entitlement, not workflow.

Marketplace-integrated listings also carry limitations worth knowing before you
commit: data clean rooms and Pub/Sub topics are not supported, billing usage
metrics do not appear in `INFORMATION_SCHEMA`, and both parties must be in a
supported Cloud Marketplace Agency Jurisdiction.

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
bq --location="${AH_LOCATION}" mk --dataset "${PROJECT_ID}:${AUDIT_DATASET:-sharing_audit}"
gcloud logging sinks create "${AUDIT_SINK:-sharing_audit_sink}" \
  "bigquery.googleapis.com/projects/${PROJECT_ID}/datasets/${AUDIT_DATASET:-sharing_audit}" \
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
FROM `your-project-id.sharing_audit.cloudaudit_googleapis_com_activity`
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

Console screens, in the order a reader would need them. Terminal output does not
belong in a deck for non-engineers — capture the console.

**Architecture / setup**

1. The **data exchange** page — `Private` discovery type visible.
2. `airport_share` dataset → **`Sharing`** → **Authorized datasets**, showing it
   authorized on gold + semantic. This is the "products, not base tables" proof.

**Publisher workflow**

3. **`Create exchange`** dialog, opened, showing the fields and the
   `Subscriber Email Logging` / `Public Discoverability` toggles. (You can cancel
   — the point is the form.)
4. The **listing page**: icon, description, categories, and the rendered
   **`Documentation`** Markdown.
5. The existing listing → `Configure data` showing **`Data Egress controls`**
   (off unless you enabled them in the console — the script does not).
6. Listing → **`Set permissions`**, showing the principal holding
   **`Analytics Hub Subscriber`** — scoped to the listing, not the project.

**Subscriber workflow**

7. Subscriber's `Sharing (Analytics Hub)` → **`Search listings`** with the
   `Private` filter applied.
8. The **`Create linked dataset`** dialog (`Project`, `Linked dataset name`).
9. Subscriber's BigQuery **`Explorer`** showing the linked dataset — note its
   **different icon**.
10. A query result against the linked dataset.
11. **The cost-isolation shot:** `INFORMATION_SCHEMA.JOBS_BY_PROJECT` run *in the
    subscriber project*, showing the job in their history with bytes billed.

**Approval & governance**

12. A viewer-only principal on the listing — no `Subscribe` available.
13. Listing → **`Manage subscriptions`**, showing an active subscription and at
    least one revoked (`STATE_INACTIVE`) one side by side. Revoked subscriptions
    accumulate across rehearsals, so expect more than one.
14. The **`Remove subscription?`** confirmation dialog.
15. Exchange → **`Usage metrics`** tab, especially **Subscribers per organization**.
16. Audit-log query results over `sharing_audit`.

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
- [Commercialize listings on Cloud Marketplace](https://docs.cloud.google.com/bigquery/docs/analytics-hub-cloud-marketplace)
- [Analytics Hub IAM roles](https://docs.cloud.google.com/iam/docs/roles-permissions/analyticshub)
- [VPC Service Controls rules for Analytics Hub](https://docs.cloud.google.com/bigquery/docs/analytics-hub-vpc-sc-rules)
- [Sharing audit logging](https://docs.cloud.google.com/bigquery/docs/analytics-hub-audit-logging)
- [Monitor listings (usage metrics)](https://docs.cloud.google.com/bigquery/docs/analytics-hub-monitor-listings)
- [Authorized datasets](https://docs.cloud.google.com/bigquery/docs/authorized-datasets)
