#!/usr/bin/env bash
#
# Thin wrapper around scripts/generate_data_insights.py so the documented command
# `bash scripts/generate_data_insights.sh [--dataset-insights]` keeps working.
# The real implementation (Dataplex DATA_DOCUMENTATION / DATA_PROFILE scans with
# proper LRO + job polling) lives in the Python file -- see its header for details.
#
# Usage:
#   source .env && bash scripts/generate_data_insights.sh [--dataset-insights]
#
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "${HERE}/generate_data_insights.py" "$@"
