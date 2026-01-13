# Engage Analytics dbt Project

Transforms healthcare FHIR data from Airbyte into analytical models.

## Prerequisites

- PostgreSQL database with Airbyte-synced FHIR data
- Python 3.10+
- [uv](https://docs.astral.sh/uv/) package manager

## Setup

### 1. Install dbt globally

```bash
uv tool install dbt-core --with dbt-postgres
```

Verify installation:
```bash
dbt --version
```

### 2. Set up the database

Start the PostgreSQL container:
```bash
cd /path/to/engage-analytics
docker compose up -d
```

Load the schema (if using the provided SQL dump):
```bash
docker exec -i nycdb psql -U postgres -d airbyte < dbt/db/airbyte_schema.sql
```

### 3. Configure environment

Copy and edit `.envrc` with your database settings:

```bash
export DBT_HOST=localhost
export DBT_PORT=5434          # Default Docker port
export DBT_USER=postgres
export DBT_PASSWORD=engage    # Your database password
export DBT_DATABASE=airbyte
export DBT_SCHEMA=engage_analytics
```

Load the environment:
```bash
source .envrc
# Or if using direnv: direnv allow
```

### 4. Install dbt packages

```bash
cd dbt
dbt deps --profiles-dir .
```

### 5. Run dbt

Load seed data (questionnaire metadata):
```bash
dbt seed --profiles-dir .
```

Build all models:
```bash
dbt run --profiles-dir .
```

## Project Structure

```
dbt/
├── models/
│   ├── staging/        # Raw data extraction
│   ├── intermediate/   # Business logic transformations
│   └── marts/          # Final analytical models
│       ├── qr_named/   # Questionnaire responses (wide format)
│       └── resources/  # FHIR resources (patient, encounter, etc.)
├── macros/             # Reusable SQL functions
├── seeds/              # Reference data (questionnaire_metadata.csv)
└── profiles.yml        # Database connection config
```

## Common Commands

```bash
# Run specific model
dbt run --profiles-dir . --select model_name

# Run models and their dependencies
dbt run --profiles-dir . --select +model_name

# Compile SQL without running
dbt compile --profiles-dir .

# Test models
dbt test --profiles-dir .
```

## Schemas

The project creates multiple schemas:
- `engage_analytics_engage_analytics_stg` - Staging models
- `engage_analytics_engage_analytics_int` - Intermediate models
- `engage_analytics_engage_analytics_mart` - Final mart models
