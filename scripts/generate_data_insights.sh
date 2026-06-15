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
REPO_ROOT="$(cd "${HERE}/.." && pwd)"
# Run via uv so the Python version is pinned/reproducible (see pyproject.toml).
# The script itself is stdlib-only, but uv keeps invocation consistent.
exec uv run --project "${REPO_ROOT}" python "${HERE}/generate_data_insights.py" "$@"
