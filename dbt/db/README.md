# Database Seed Data

This folder contains a PostgreSQL dump of the `airbyte` schema with sample FHIR data for local development.

## Contents

- `airbyte_schema.sql` - Full dump of the airbyte schema (tables + data)

## Data Summary

| Table | Rows |
|-------|------|
| patient | 400 |
| questionnaire_response | 2,384 |
| encounter | 1,082 |
| care_plan | 588 |
| practitioner | 72 |
| location | 15 |
| organization | 13 |
| care_team | 11 |

Data was extracted from FHIR server via Airbyte (Aug-Sep 2025).

## Importing to a New PostgreSQL Instance

### Prerequisites

- PostgreSQL 14+ installed
- Database created (e.g., `airbyte`)

### Import Steps

```bash
# 1. Create the database (if it doesn't exist)
createdb -h localhost -U postgres airbyte

# 2. Import the dump
psql -h localhost -U postgres -d airbyte -f airbyte_schema.sql

# 3. Verify
psql -h localhost -U postgres -d airbyte -c "SELECT count(*) FROM airbyte.patient;"
```

### After Import

Run dbt to create all the transformed views/tables:

```bash
cd /path/to/engage-analytics/dbt
source .envrc
uv run dbt seed --profiles-dir .
uv run dbt run --profiles-dir .
```

## Updating the Dump

To refresh this dump from a running database:

```bash
cd /path/to/engage-analytics/dbt
source .envrc
PGPASSWORD=$DBT_PASSWORD pg_dump -h $DBT_HOST -p $DBT_PORT -U $DBT_USER -d $DBT_DATABASE \
  --schema=airbyte --no-owner --no-acl > db/airbyte_schema.sql
```
