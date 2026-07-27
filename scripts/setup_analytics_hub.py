#!/usr/bin/env python3
"""Publisher-side Analytics Hub setup for the NIO data-sharing showcase.

Publishes the curated share dataset (the shr_* authorized views built by the
Dataform `share` stage) as a private BigQuery Analytics Hub **Data Exchange
(DCX)** listing, and whitelists a subscriber principal. This is the "hub" side of
the NIO hub-and-spoke model: the publisher (CAG) owns the storage; subscribers
(spokes) link the dataset in their own project and pay for their own queries.

Steps:
    1. Authorize the share dataset onto each source dataset (gold/semantic) so
       the shr_* views can read their base tables once linked.
    2. Create the Data Exchange (idempotent).
    3. Create a listing over the whole share dataset (idempotent).
    4. Grant roles/analyticshub.subscriber to the subscriber principal on the
       listing (per-listing "whitelisting"; never granted project-wide).

Adapted from data-clean-room-demo/setup_ah_dcx.py (generalized: no hardcoded
dataset choices, configurable display names, added source-dataset authorization).

Analytics Hub resources MUST be created in the same location as the shared
dataset's region (e.g. us-central1), not the "US" multi-region.

Usage:
    uv run python scripts/setup_analytics_hub.py \\
        --publisher-project-id my-hub-project \\
        --share-dataset airport_share \\
        --source-datasets airport_gold,airport_semantic \\
        --location us-central1 \\
        --exchange-id nio_exchange \\
        --listing-id airport_ops_daily \\
        --subscriber-principal user:partner@example.com
"""

import argparse
import sys

from google.cloud import bigquery
from google.cloud import bigquery_analyticshub_v1
from google.cloud.bigquery_analyticshub_v1 import types
from google.cloud.exceptions import GoogleCloudError


def authorize_share_dataset(
    publisher_project_id: str,
    share_dataset: str,
    source_datasets: list[str],
) -> None:
    """Authorize the share dataset to read each source dataset's views.

    The shr_* views live in `share_dataset` but SELECT from tables/views in the
    gold and semantic datasets. Adding the share dataset as an authorized dataset
    lets those views resolve without granting subscribers access to the base data.
    """
    client = bigquery.Client(project=publisher_project_id)

    # Authorized-dataset entries are represented as an AccessEntry whose entity is
    # a dict: {"dataset": {projectId, datasetId}, "targetTypes": ["VIEWS"]}. The
    # installed google-cloud-bigquery has no dedicated DatasetAccessEntry class.
    authorized_entity = {
        "dataset": {
            "projectId": publisher_project_id,
            "datasetId": share_dataset,
        },
        "targetTypes": ["VIEWS"],
    }

    for source in source_datasets:
        source = source.strip()
        if not source:
            continue
        try:
            source_ds = client.get_dataset(
                bigquery.DatasetReference(publisher_project_id, source)
            )
            entries = list(source_ds.access_entries)

            already = any(
                e.entity_type == "dataset"
                and isinstance(e.entity_id, dict)
                and e.entity_id.get("dataset", {}).get("datasetId") == share_dataset
                for e in entries
            )
            if already:
                print(f"⚠ Share dataset already authorized on '{source}'")
                continue

            entries.append(
                bigquery.AccessEntry(
                    role=None, entity_type="dataset", entity_id=authorized_entity
                )
            )
            source_ds.access_entries = entries
            client.update_dataset(source_ds, ["access_entries"])
            print(f"✓ Authorized share dataset '{share_dataset}' to read '{source}'")
        except Exception as e:  # noqa: BLE001 - surface a clear, actionable message
            print(f"⚠ Could not authorize '{share_dataset}' on '{source}': {e}")
            print("  The listing will publish, but linked views may not resolve")
            print("  until the share dataset is authorized on the source dataset.")


def create_exchange(
    client: bigquery_analyticshub_v1.AnalyticsHubServiceClient,
    project_id: str,
    location: str,
    exchange_id: str,
    display_name: str,
) -> str:
    """Create a private Data Exchange (idempotent)."""
    parent = f"projects/{project_id}/locations/{location}"
    exchange = types.DataExchange(
        {
            "display_name": display_name,
            "description": (
                "Private data exchange for the NIO hub-and-spoke data-sharing "
                "model: the publisher owns storage, subscribers pay their own "
                "compute."
            ),
            "primary_contact": "data-sharing-admin@example.com",
        }
    )
    print(f"Creating Data Exchange '{exchange_id}' in {parent}...")
    try:
        request = types.CreateDataExchangeRequest(
            parent=parent,
            data_exchange_id=exchange_id,
            data_exchange=exchange,
        )
        operation = client.create_data_exchange(request=request)
        print(f"✓ Data Exchange created: {operation.name}")
        return operation.name
    except GoogleCloudError as e:
        if "already exists" in str(e).lower():
            name = f"{parent}/dataExchanges/{exchange_id}"
            print(f"⚠ Data Exchange already exists: {name}")
            return name
        raise


def create_listing(
    client: bigquery_analyticshub_v1.AnalyticsHubServiceClient,
    exchange_name: str,
    listing_id: str,
    publisher_project_id: str,
    share_dataset: str,
    display_name: str,
) -> str:
    """Create a listing over the whole share dataset (idempotent)."""
    print(f"Creating listing '{listing_id}' over dataset '{share_dataset}'...")
    try:
        listing = types.Listing(
            display_name=display_name,
            description=(
                f"Curated airport operations data products ({share_dataset}) "
                "shared with whitelisted subscribers."
            ),
            primary_contact="data-sharing-admin@example.com",
            bigquery_dataset=types.Listing.BigQueryDatasetSource(
                dataset=f"projects/{publisher_project_id}/datasets/{share_dataset}"
            ),
        )
        request = types.CreateListingRequest(
            parent=exchange_name,
            listing_id=listing_id,
            listing=listing,
        )
        operation = client.create_listing(request=request)
        print(f"✓ Listing created: {operation.name}")
        return operation.name
    except GoogleCloudError as e:
        if "already exists" in str(e).lower():
            name = f"{exchange_name}/listings/{listing_id}"
            print(f"⚠ Listing already exists: {name}")
            return name
        raise


def grant_subscriber(
    client: bigquery_analyticshub_v1.AnalyticsHubServiceClient,
    listing_name: str,
    subscriber_principal: str,
) -> None:
    """Grant roles/analyticshub.subscriber on the listing (per-listing only)."""
    from google.iam.v1 import iam_policy_pb2
    from google.iam.v1 import policy_pb2

    member = subscriber_principal if ":" in subscriber_principal else f"user:{subscriber_principal}"
    print(f"Whitelisting subscriber {member} on the listing...")
    try:
        policy = client.get_iam_policy(
            request=iam_policy_pb2.GetIamPolicyRequest(resource=listing_name)
        )
        role = "roles/analyticshub.subscriber"
        binding = next((b for b in policy.bindings if b.role == role), None)
        if binding is None:
            policy.bindings.append(policy_pb2.Binding(role=role, members=[member]))
            print(f"✓ Created subscriber binding for {member}")
        elif member not in binding.members:
            binding.members.append(member)
            print(f"✓ Added {member} to existing subscriber binding")
        else:
            print(f"⚠ {member} already has subscriber access")
            return
        client.set_iam_policy(
            request=iam_policy_pb2.SetIamPolicyRequest(
                resource=listing_name, policy=policy
            )
        )
        print("✓ Listing IAM policy updated")
    except GoogleCloudError as e:
        print(f"✗ Error setting listing IAM policy: {e}")
        raise


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Publish the curated share dataset via Analytics Hub (NIO hub side)."
    )
    parser.add_argument("--publisher-project-id", required=True)
    parser.add_argument("--share-dataset", required=True)
    parser.add_argument(
        "--source-datasets",
        required=True,
        help="Comma-separated source datasets the share views read (e.g. airport_gold,airport_semantic).",
    )
    parser.add_argument("--location", default="us-central1")
    parser.add_argument("--exchange-id", required=True)
    parser.add_argument("--exchange-display-name", default="NIO Airport Operations Data Exchange")
    parser.add_argument("--listing-id", required=True)
    parser.add_argument("--listing-display-name", default="Airport Operations - Curated Share")
    parser.add_argument(
        "--subscriber-principal",
        required=True,
        help="IAM principal to whitelist (user:/group:/serviceAccount:).",
    )
    args = parser.parse_args()

    print("=" * 70)
    print("NIO Data Sharing - Analytics Hub Publisher Setup (DCX)")
    print("=" * 70)
    print(f"Publisher project : {args.publisher_project_id}")
    print(f"Share dataset     : {args.share_dataset}")
    print(f"Source datasets   : {args.source_datasets}")
    print(f"Location          : {args.location}")
    print(f"Exchange / Listing: {args.exchange_id} / {args.listing_id}")
    print(f"Subscriber        : {args.subscriber_principal}")
    print()

    try:
        authorize_share_dataset(
            args.publisher_project_id,
            args.share_dataset,
            args.source_datasets.split(","),
        )
        client = bigquery_analyticshub_v1.AnalyticsHubServiceClient()
        exchange_name = create_exchange(
            client,
            args.publisher_project_id,
            args.location,
            args.exchange_id,
            args.exchange_display_name,
        )
        listing_name = create_listing(
            client,
            exchange_name,
            args.listing_id,
            args.publisher_project_id,
            args.share_dataset,
            args.listing_display_name,
        )
        grant_subscriber(client, listing_name, args.subscriber_principal)

        print("\n" + "=" * 70)
        print("✓ SUCCESS: Publisher listing is live.")
        print("=" * 70)
        print(f"Exchange: {exchange_name}")
        print(f"Listing : {listing_name}")
        print()
        print("Subscriber next steps (run scripts/subscribe_analytics_hub.py in the spoke):")
        print("  - needs roles/analyticshub.subscriptionOwner + roles/bigquery.user")
        print("    in its OWN project to create the linked dataset")
        print("=" * 70)
    except Exception as e:  # noqa: BLE001
        print(f"\n✗ ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
