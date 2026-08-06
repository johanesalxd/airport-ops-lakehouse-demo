#!/usr/bin/env bash
#
# Thin wrapper around scripts/setup_analytics_hub.py (publisher / hub side).
# Reads the Analytics Hub config from .env and publishes the curated share
# dataset as a private Data Exchange listing, whitelisting the subscriber.
#
# Usage:
#   source .env && bash scripts/setup_analytics_hub.sh
#
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${HERE}/.." && pwd)"

: "${PROJECT_ID:?set PROJECT_ID (source .env)}"
: "${DS_GOLD:?set DS_GOLD}"
: "${DS_SEMANTIC:?set DS_SEMANTIC}"
: "${DS_SHARE:?set DS_SHARE}"
: "${AH_LOCATION:?set AH_LOCATION}"
: "${ANALYTICS_HUB_EXCHANGE:?set ANALYTICS_HUB_EXCHANGE}"
: "${ANALYTICS_HUB_LISTING:?set ANALYTICS_HUB_LISTING}"
: "${SUBSCRIBER_PRINCIPAL:?set SUBSCRIBER_PRINCIPAL}"
: "${AH_PRIMARY_CONTACT:?set AH_PRIMARY_CONTACT (shown to subscribers in the console)}"
: "${ANALYTICS_HUB_EXCHANGE_DISPLAY:=Partner Data Exchange}"
: "${ANALYTICS_HUB_LISTING_DISPLAY:=Airport Operations - Curated Share}"

exec uv run --project "${REPO_ROOT}" python "${HERE}/setup_analytics_hub.py" \
  --publisher-project-id "${PROJECT_ID}" \
  --share-dataset "${DS_SHARE}" \
  --source-datasets "${DS_GOLD},${DS_SEMANTIC}" \
  --location "${AH_LOCATION}" \
  --exchange-id "${ANALYTICS_HUB_EXCHANGE}" \
  --exchange-display-name "${ANALYTICS_HUB_EXCHANGE_DISPLAY}" \
  --listing-id "${ANALYTICS_HUB_LISTING}" \
  --listing-display-name "${ANALYTICS_HUB_LISTING_DISPLAY}" \
  --subscriber-principal "${SUBSCRIBER_PRINCIPAL}" \
  --primary-contact "${AH_PRIMARY_CONTACT}"
