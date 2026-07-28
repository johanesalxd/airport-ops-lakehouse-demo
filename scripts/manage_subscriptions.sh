#!/usr/bin/env bash
#
# Thin wrapper around scripts/manage_subscriptions.py (publisher governance).
# Lists or revokes subscriptions on the listing, reading config from .env.
#
# Usage:
#   source .env && bash scripts/manage_subscriptions.sh --list
#   source .env && bash scripts/manage_subscriptions.sh \
#     --revoke projects/123/locations/us-central1/subscriptions/sub_abc
#
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${HERE}/.." && pwd)"

: "${PROJECT_ID:?set PROJECT_ID (source .env)}"
: "${AH_LOCATION:?set AH_LOCATION}"
: "${ANALYTICS_HUB_EXCHANGE:?set ANALYTICS_HUB_EXCHANGE}"
: "${ANALYTICS_HUB_LISTING:?set ANALYTICS_HUB_LISTING}"

exec uv run --project "${REPO_ROOT}" python "${HERE}/manage_subscriptions.py" \
  --publisher-project-id "${PROJECT_ID}" \
  --location "${AH_LOCATION}" \
  --exchange-id "${ANALYTICS_HUB_EXCHANGE}" \
  --listing-id "${ANALYTICS_HUB_LISTING}" \
  "$@"
