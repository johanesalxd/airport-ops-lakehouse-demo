#!/usr/bin/env bash
#
# Thin wrapper around scripts/generate_data_quality.py so the documented command
# `bash scripts/generate_data_quality.sh [FLAGS]` keeps working.
# The real implementation (Dataplex DATA_QUALITY scans with profile-based rule
# recommendations, BigQuery export, and catalog publishing) lives in the Python
# file -- see its header for details.
#
# Usage:
#   source .env && bash scripts/generate_data_quality.sh [--dry-run | --recommend-only]
#
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${HERE}/.." && pwd)"
exec uv run --project "${REPO_ROOT}" python "${HERE}/generate_data_quality.py" "$@"
