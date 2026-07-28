#!/usr/bin/env python3
"""Publisher-side subscription governance for the data-sharing showcase.

This is the "data-owner" control surface for a private listing. In Analytics Hub
a private listing has no separate pending/approve queue (that "Request access"
flow is a commercial/Marketplace feature); the data owner instead governs access
by:

    1. Admission  — granting roles/analyticshub.subscriber on the listing
       (done by scripts/setup_analytics_hub.py; that IS the approval decision).
    2. Visibility — listing who has subscribed (this script, --list).
    3. Revocation — revoking a subscription (this script, --revoke), which
       detaches the subscriber's linked dataset.

Requires roles/analyticshub.admin (or listingAdmin) on the listing.

Usage:
    # Who has subscribed to the listing?
    uv run python scripts/manage_subscriptions.py \\
        --publisher-project-id my-hub-project \\
        --location us-central1 \\
        --exchange-id partner_exchange \\
        --listing-id airport_ops_daily \\
        --list

    # Revoke one subscription (detaches its linked dataset):
    uv run python scripts/manage_subscriptions.py ... \\
        --revoke projects/123/locations/us-central1/subscriptions/sub_abc
"""

import argparse
import sys

from google.cloud import bigquery_analyticshub_v1
from google.cloud.bigquery_analyticshub_v1 import types


def list_subscriptions(
    client: bigquery_analyticshub_v1.AnalyticsHubServiceClient,
    listing_name: str,
) -> None:
    """Print every subscription on the listing (the publisher's visibility view)."""
    print(f"Subscriptions on {listing_name}:\n")
    request = types.ListSharedResourceSubscriptionsRequest(resource=listing_name)
    found = False
    for sub in client.list_shared_resource_subscriptions(request=request):
        found = True
        contact = sub.subscriber_contact or "(no contact)"
        org = sub.organization_display_name or "(no org)"
        print(f"  • {sub.name}")
        print(f"      state   : {sub.state.name}")
        print(f"      org     : {org}")
        print(f"      contact : {contact}")
        if sub.linked_dataset_map:
            for project, linked in sub.linked_dataset_map.items():
                print(f"      linked  : {project} -> {linked.linked_dataset}")
    if not found:
        print("  (none yet — no one has subscribed to this listing)")


def revoke_subscription(
    client: bigquery_analyticshub_v1.AnalyticsHubServiceClient,
    subscription_name: str,
) -> None:
    """Revoke a subscription, detaching the subscriber's linked dataset."""
    print(f"Revoking subscription {subscription_name} ...")
    client.revoke_subscription(
        request=types.RevokeSubscriptionRequest(name=subscription_name)
    )
    print("✓ Revoked; the subscriber's linked dataset is detached.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="List or revoke subscriptions on a listing (publisher governance)."
    )
    parser.add_argument("--publisher-project-id", required=True)
    parser.add_argument("--location", default="us-central1")
    parser.add_argument("--exchange-id", required=True)
    parser.add_argument("--listing-id", required=True)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--list", action="store_true", help="List all subscriptions on the listing."
    )
    group.add_argument(
        "--revoke",
        metavar="SUBSCRIPTION_NAME",
        help="Full resource name of the subscription to revoke.",
    )
    args = parser.parse_args()

    listing_name = (
        f"projects/{args.publisher_project_id}/locations/{args.location}"
        f"/dataExchanges/{args.exchange_id}/listings/{args.listing_id}"
    )

    print("=" * 70)
    print("Hub-and-Spoke Data Sharing - Subscription Governance (publisher)")
    print("=" * 70)
    print(f"Listing: {listing_name}\n")

    try:
        client = bigquery_analyticshub_v1.AnalyticsHubServiceClient()
        if args.revoke:
            revoke_subscription(client, args.revoke)
        else:
            list_subscriptions(client, listing_name)
    except Exception as e:  # noqa: BLE001
        print(f"\n✗ ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
