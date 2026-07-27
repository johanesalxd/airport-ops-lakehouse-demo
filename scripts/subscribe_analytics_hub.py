#!/usr/bin/env python3
"""Subscriber-side Analytics Hub step for the NIO data-sharing showcase.

Runs as the "spoke": subscribes to the publisher's listing, which creates a
read-only **linked dataset** in the subscriber's own project, then runs a sample
query billed to the subscriber project to demonstrate **cost isolation** (the
publisher stores the data; the subscriber pays for its own compute).

Requires, in the SUBSCRIBER project:
    * roles/analyticshub.subscriptionOwner (create the subscription)
    * roles/bigquery.user (create the linked dataset + run queries)
and roles/analyticshub.subscriber on the listing (granted by the publisher via
scripts/setup_analytics_hub.py).

Usage:
    uv run python scripts/subscribe_analytics_hub.py \\
        --subscriber-project-id my-spoke-project \\
        --publisher-project-id my-hub-project \\
        --location us-central1 \\
        --exchange-id nio_exchange \\
        --listing-id airport_ops_daily \\
        --linked-dataset airport_ops_shared \\
        --sample-view shr_airport_operations_daily
"""

import argparse
import sys

from google.cloud import bigquery
from google.cloud import bigquery_analyticshub_v1
from google.cloud.bigquery_analyticshub_v1 import types
from google.cloud.exceptions import GoogleCloudError


def subscribe(
    client: bigquery_analyticshub_v1.AnalyticsHubServiceClient,
    listing_name: str,
    subscriber_project_id: str,
    linked_dataset: str,
    location: str,
) -> None:
    """Subscribe to the listing, creating a linked dataset in the spoke project."""
    print(f"Subscribing to {listing_name}")
    print(f"  -> linked dataset {subscriber_project_id}:{linked_dataset} ({location})")
    request = types.SubscribeListingRequest(
        name=listing_name,
        destination_dataset=types.DestinationDataset(
            dataset_reference=types.DestinationDatasetReference(
                project_id=subscriber_project_id,
                dataset_id=linked_dataset,
            ),
            location=location,
        ),
    )
    try:
        client.subscribe_listing(request=request)
        print("✓ Subscribed; linked dataset created")
    except GoogleCloudError as e:
        if "already exists" in str(e).lower():
            print("⚠ Linked dataset already exists (already subscribed)")
        else:
            raise


def run_cost_isolated_query(
    subscriber_project_id: str,
    linked_dataset: str,
    sample_view: str,
) -> None:
    """Query the linked view, billed to the subscriber project (cost isolation)."""
    client = bigquery.Client(project=subscriber_project_id)
    table = f"`{subscriber_project_id}.{linked_dataset}.{sample_view}`"
    sql = f"SELECT * FROM {table} LIMIT 10"
    print(f"\nRunning sample query billed to '{subscriber_project_id}':")
    print(f"  {sql}")
    job = client.query(sql)
    rows = list(job.result())
    print(f"✓ Query job {job.job_id} ran in project '{job.project}'")
    print(f"  rows returned : {len(rows)}")
    print(f"  bytes billed  : {job.total_bytes_billed}")
    print("  ^ billed to the SUBSCRIBER project = cost isolation")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Subscribe to a NIO Analytics Hub listing (spoke side) and prove cost isolation."
    )
    parser.add_argument("--subscriber-project-id", required=True)
    parser.add_argument("--publisher-project-id", required=True)
    parser.add_argument("--location", default="us-central1")
    parser.add_argument("--exchange-id", required=True)
    parser.add_argument("--listing-id", required=True)
    parser.add_argument("--linked-dataset", default="airport_ops_shared")
    parser.add_argument("--sample-view", default="shr_airport_operations_daily")
    parser.add_argument(
        "--skip-query",
        action="store_true",
        help="Only subscribe; skip the cost-isolation sample query.",
    )
    args = parser.parse_args()

    listing_name = (
        f"projects/{args.publisher_project_id}/locations/{args.location}"
        f"/dataExchanges/{args.exchange_id}/listings/{args.listing_id}"
    )

    print("=" * 70)
    print("NIO Data Sharing - Analytics Hub Subscriber (spoke)")
    print("=" * 70)
    print(f"Subscriber project: {args.subscriber_project_id}")
    print(f"Listing           : {listing_name}")
    print(f"Linked dataset    : {args.linked_dataset}")
    print()

    try:
        client = bigquery_analyticshub_v1.AnalyticsHubServiceClient()
        subscribe(
            client,
            listing_name,
            args.subscriber_project_id,
            args.linked_dataset,
            args.location,
        )
        if not args.skip_query:
            run_cost_isolated_query(
                args.subscriber_project_id,
                args.linked_dataset,
                args.sample_view,
            )
        print("\n" + "=" * 70)
        print("✓ SUCCESS: Subscription complete.")
        print("=" * 70)
    except Exception as e:  # noqa: BLE001
        print(f"\n✗ ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
