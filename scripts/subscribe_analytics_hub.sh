#!/usr/bin/env bash
#
# Thin wrapper around scripts/subscribe_analytics_hub.py (subscriber / spoke
# side). Subscribes to the publisher's listing (creating a linked dataset in the
# subscriber project) and runs a cost-isolated sample query.
#
# Usage:
#   source .env && bash scripts/subscribe_analytics_hub.sh [--skip-query]
#
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${HERE}/.." && pwd)"

: "${PROJECT_ID:?set PROJECT_ID (source .env)}"
: "${AH_LOCATION:?set AH_LOCATION}"
: "${ANALYTICS_HUB_EXCHANGE:?set ANALYTICS_HUB_EXCHANGE}"
: "${ANALYTICS_HUB_LISTING:?set ANALYTICS_HUB_LISTING}"
: "${SUBSCRIBER_PROJECT:?set SUBSCRIBER_PROJECT}"
: "${SUBSCRIBER_LINKED_DATASET:=airport_ops_shared}"

exec uv run --project "${REPO_ROOT}" python "${HERE}/subscribe_analytics_hub.py" \
  --subscriber-project-id "${SUBSCRIBER_PROJECT}" \
  --publisher-project-id "${PROJECT_ID}" \
  --location "${AH_LOCATION}" \
  --exchange-id "${ANALYTICS_HUB_EXCHANGE}" \
  --listing-id "${ANALYTICS_HUB_LISTING}" \
  --linked-dataset "${SUBSCRIBER_LINKED_DATASET}" \
  "$@"
