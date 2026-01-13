#!/bin/bash
# ABOUTME: Wrapper script that refreshes dbt models then exports to S3.
# ABOUTME: Usage: ./run_export.sh [--type anon|pii|both]

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DBT_DIR="$(dirname "$SCRIPT_DIR")/dbt"

# Load environment variables from dataexport/.env
set -a
source "$SCRIPT_DIR/.env"
set +a

# Default export type
EXPORT_TYPE="${1:-both}"

echo "============================================================"
echo "Step 1: Refreshing dbt models"
echo "============================================================"
cd "$DBT_DIR"
uv run dbt run --profiles-dir .

echo ""
echo "============================================================"
echo "Step 2: Exporting to S3"
echo "============================================================"
cd "$SCRIPT_DIR"
uv run python export_to_s3.py --type "$EXPORT_TYPE"

echo ""
echo "============================================================"
echo "Complete!"
echo "============================================================"
