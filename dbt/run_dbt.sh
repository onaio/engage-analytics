#!/bin/bash
# Load environment variables from .env file
export $(cat .env | xargs)

# Run dbt with all arguments passed to this script
uv run dbt "$@" --profiles-dir .