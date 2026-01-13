[//]: # (ABOUTME: Guide for data analysts on how Engage Analytics works.)
[//]: # (ABOUTME: Covers data flow, DBT transformations, metrics, and local development.)

# Engage Analytics Guide

A guide for data analysts on how Engage Analytics works, from data collection through transformation to exports, and how to modify metrics.

---

## Table of Contents

1. [Introduction](#introduction)
2. [System Architecture](#system-architecture)
3. [DBT Transformation Pipeline](#dbt-transformation-pipeline)
4. [PII vs Anonymized Data](#pii-vs-anonymized-data)
5. [Metrics System](#metrics-system)
6. [Maintenance & Update Workflow](#maintenance--update-workflow)
7. [How to Add or Modify Metrics](#how-to-add-or-modify-metrics)
8. [Local Development Setup](#local-development-setup)
9. [S3 Access & Permissions](#s3-access--permissions)
10. [Reference](#reference)

---

## Introduction

Engage Analytics is a data pipeline that transforms healthcare data from FHIR servers into analytical models and metrics. The system:

- Extracts FHIR resources (patients, encounters, questionnaire responses, etc.)
- Transforms raw JSON into structured, queryable tables
- Generates both PII-containing and anonymized datasets
- Produces aggregate metrics for dashboards and reporting
- Exports data to S3 for downstream consumption

### What is FHIR?

FHIR (Fast Healthcare Interoperability Resources) is a standard for exchanging healthcare data electronically. FHIR represents clinical data as "resources" — structured JSON objects like Patient, Encounter, Observation, and QuestionnaireResponse. Each resource has a defined schema with nested fields, arrays, and coded values. This pipeline flattens these complex JSON structures into relational tables suitable for analytics.

---

## System Architecture

### End-to-End Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          ENGAGE ANALYTICS PIPELINE                               │
└─────────────────────────────────────────────────────────────────────────────────┘

     ┌────────────┐         ┌────────────┐         ┌────────────┐         ┌────────────┐
     │   FHIR     │         │ PostgreSQL │         │    DBT     │         │     S3     │
     │  Server    │────────▶│  Database  │────────▶│ Transforms │────────▶│  Buckets   │
     │            │         │            │         │            │         │            │
     └────────────┘         └────────────┘         └────────────┘         └────────────┘
           │                      │                      │                      │
           │    Airbyte           │                      │                      │
           │    syncs FHIR        │    Raw JSON          │    Flat tables       │
           │    resources         │    stored as         │    + metrics         │
           │    daily             │    JSONB columns     │    exported as       │
           │                      │                      │    CSV ZIPs          │
           └──────────────────────┘                      └──────────────────────┘
                                                                │
                                                                │
                         ┌──────────────────────────────────────┼──────────────────┐
                         │                                      │                  │
                         ▼                                      ▼                  ▼
                  ┌─────────────┐                       ┌─────────────┐    ┌─────────────┐
                  │    ANON     │                       │     PII     │    │   METRICS   │
                  │   Bucket    │                       │   Bucket    │    │   Bucket    │
                  │             │                       │             │    │             │
                  │ De-identified                       │ Full data   │    │ Aggregated  │
                  │ data only   │                       │ restricted  │    │ KPIs        │
                  └─────────────┘                       └─────────────┘    └─────────────┘
```

### Components

| Component | Description |
|-----------|-------------|
| **FHIR Server** | Source system storing clinical data (patients, encounters, questionnaires) |
| **Airbyte** | ETL tool that syncs FHIR resources to PostgreSQL as raw JSON |
| **PostgreSQL** | Database storing both raw FHIR JSON and transformed tables |
| **DBT** | Transformation layer that converts JSON to flat tables and computes metrics |
| **S3** | Cloud storage for exported data, organized into three access-controlled buckets |

---

## DBT Transformation Pipeline

DBT transforms data through three layers: **Staging** → **Intermediate** → **Marts**.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          DBT TRANSFORMATION FLOW                                 │
└─────────────────────────────────────────────────────────────────────────────────┘

  RAW FHIR JSON              STAGING                  INTERMEDIATE              MARTS
  ──────────────             ───────                  ────────────              ─────

  ┌──────────────┐       ┌──────────────┐                                 ┌──────────────┐
  │ patient      │       │ stg_patient  │                                 │   patient    │
  │ {            │──────▶│              │────────────────────────────────▶│   (final)    │
  │   "name":[...│       │ Extracts:    │                                 └──────────────┘
  │   "gender"...│       │ - id         │
  │ }            │       │ - gender     │
  └──────────────┘       │ - birth_date │
                         │ - name       │
                         │ - phone      │
                         └──────────────┘


  ┌──────────────┐       ┌──────────────┐       ┌──────────────┐       ┌──────────────┐
  │ questionnaire│       │ stg_question │       │ int_qr_      │       │  qr_gad7_    │
  │ _response    │──────▶│ naire_resp   │──────▶│ answers_long │──────▶│  session_1   │
  │ {            │       │              │       │              │       ├──────────────┤
  │   "item":[   │       │ Filters to   │       │ Recursive    │       │  qr_phq9_    │
  │     nested...│       │ completed    │       │ pivot of     │       │  session_1   │
  │   ]          │       │ responses    │       │ nested items │       ├──────────────┤
  │ }            │       │              │       │ into long    │       │  qr_regist   │
  └──────────────┘       └──────────────┘       │ format       │       │  ration_info │
                                                └──────────────┘       └──────────────┘
                                                       │
                                                       │ PII masking
                                                       ▼
                                                ┌──────────────┐
                                                │  qr_gad7_    │
                                                │  session_1   │
                                                │  _anon       │
                                                ├──────────────┤
                                                │  qr_phq9_    │
                                                │  session_1   │
                                                │  _anon       │
                                                └──────────────┘
```

### Layer Details

| Layer | Schema | Purpose | Examples |
|-------|--------|---------|----------|
| **Staging** | `engage_analytics_stg` | Extract fields from raw FHIR JSON | `stg_patient`, `stg_encounter`, `stg_questionnaire_response` |
| **Intermediate** | `engage_analytics_int` | Transform and reshape data | `int_qr_answers_long` (pivots nested questionnaire items) |
| **Marts** | `engage_analytics_mart` | Final analytical tables | `patient`, `encounters`, `qr_gad7_session_1`, `qr_registration_info` |

### Source Tables

Data arrives from Airbyte into these raw tables:

| Table | Description |
|-------|-------------|
| `patient` | Patient demographics and identifiers |
| `encounter` | Clinical visits and sessions |
| `questionnaire_response` | Completed questionnaires (PHQ-9, GAD-7, etc.) |
| `practitioner` | Healthcare providers |
| `organization` | Organizations and clinics |
| `location` | Locations where care is delivered |
| `care_team` | Care team assignments |
| `care_plan` | Patient care plans |

---

## PII vs Anonymized Data

The system produces two parallel dataset tracks:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         PII vs ANONYMIZED DATA                                   │
└─────────────────────────────────────────────────────────────────────────────────┘

                    ┌───────────────────────────────────────┐
                    │         SOURCE DATA                   │
                    │  (Contains full PII)                  │
                    └───────────────────┬───────────────────┘
                                        │
                    ┌───────────────────┴───────────────────┐
                    │                                       │
                    ▼                                       ▼
        ┌───────────────────────┐           ┌───────────────────────┐
        │     NAMED TABLES      │           │   ANONYMIZED TABLES   │
        │     (Full PII)        │           │   (PII Masked)        │
        ├───────────────────────┤           ├───────────────────────┤
        │ • patient             │           │ • patient_anon        │
        │ • qr_registration_info│           │ • qr_registration_    │
        │ • qr_gad7_session_1   │           │   info_anon           │
        │ • qr_phq9_session_1   │           │ • qr_gad7_session_1   │
        │                       │           │   _anon               │
        └───────────────────────┘           └───────────────────────┘
                    │                                       │
                    ▼                                       ▼
        ┌───────────────────────┐           ┌───────────────────────┐
        │      PII Bucket       │           │      ANON Bucket      │
        │   (Restricted)        │           │   (Broader Access)    │
        └───────────────────────┘           └───────────────────────┘
```

### Masking Rules

| Field Type | Named Value | Anonymized Value |
|------------|-------------|------------------|
| **Patient ID** | `abc-123-def` | `MD5(abc-123-def)` → `5d41402abc4b2a76...` |
| **Name** | `Jane Smith` | `REDACTED` |
| **Phone** | `555-123-4567` | `XXX-XXX-4567` (last 4 shown) |
| **Date of Birth** | `1985-03-15` | Replaced with `age_years` (e.g., `39`) |
| **Email** | `jane@email.com` | `REDACTED` |
| **Address** | `123 Main St` | `REDACTED` |
| **Zip Code** | `12345` | `123XX` (first 3 only) |
| **Medicaid #** | `MCD123456` | `REDACTED` |

---

## Metrics System

Metrics are aggregate KPIs computed from the transformed data.

### How Metrics Work

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            METRICS ARCHITECTURE                                  │
└─────────────────────────────────────────────────────────────────────────────────┘

                        ┌───────────────────────────────┐
                        │    _metrics_catalog.yml       │
                        │                               │
                        │  Defines metrics:             │
                        │  - id: active_providers       │
                        │  - unit: count                │
                        │  - expression: count(...)     │
                        └───────────────┬───────────────┘
                                        │
                                        │ Jinja macro reads
                                        │ catalog at build time
                                        ▼
  ┌──────────────┐      ┌──────────────┐      ┌──────────────────────────┐
  │ patient      │      │ active_      │      │    fct_metrics_long      │
  │ encounters   │─────▶│ providers    │─────▶│                          │
  │ practitioners│      │ clients_*    │      │ period_date | metric_id  │
  │ qr_*         │      │ etc.         │      │ ------------|----------- │
  └──────────────┘      └──────────────┘      │ 2025-01-01  | active_... │
                                              │ 2025-01-01  | patient_.. │
                                              │ 2025-01-02  | active_... │
                                              └──────────────────────────┘
```

### Available Metrics

| Metric | Description |
|--------|-------------|
| `provider_count` | Total unique providers |
| `patient_count` | Total unique patients |
| `active_providers` | Providers with questionnaire submissions in past 30 days |
| `percent_active_providers` | Percentage of providers active in past 30 days |
| `client_encounters` | Unique clients seen per day |
| `clients_eligible_ipc` | Clients eligible for Interpersonal Counseling |
| `clients_accepted_ipc` | Clients who accepted IPC |
| `clients_eligible_fws` | Clients eligible for Financial Wellness Services |
| `clients_accepted_fws` | Clients who accepted FWS |
| `encounters_by_delivery_format` | Encounters broken down by delivery method |

### Metrics Fact Table

All metrics are combined into `fct_metrics_long` for easy querying:

```sql
SELECT period_date, metric_id, value
FROM fct_metrics_long
WHERE organization_id = 'org-123'
  AND metric_id = 'active_providers'
ORDER BY period_date;
```

---

## Maintenance & Update Workflow

This diagram shows how you (the data analyst) can modify metrics and how changes flow to production.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                      MAINTENANCE & UPDATE WORKFLOW                               │
└─────────────────────────────────────────────────────────────────────────────────┘

    CLOUD SERVER                                           YOU (Data Analyst)
    ════════════                                           ══════════════════

    Runs daily via cron                                    Your local machine

    ┌─────────────────────────┐                            ┌─────────────────────┐
    │  1. Pull latest code    │◀───────────────────────────│  5. Push changes    │
    │     from GitHub         │         git push           │     to GitHub       │
    └───────────┬─────────────┘                            └──────────▲──────────┘
                │                                                     │
                ▼                                                     │
    ┌─────────────────────────┐                            ┌──────────┴──────────┐
    │  2. Run DBT             │                            │  4. Commit changes  │
    │     dbt run             │                            │     git commit      │
    │     (refresh all models)│                            └──────────▲──────────┘
    └───────────┬─────────────┘                                       │
                │                                                     │
                ▼                                          ┌──────────┴──────────┐
    ┌─────────────────────────┐                            │  3. Modify DBT      │
    │  3. Export to S3        │                            │     - Edit SQL      │
    │     ./run_export.sh all │                            │     - Add metric    │
    │                         │                            │     - Update catalog│
    └───────────┬─────────────┘                            └──────────▲──────────┘
                │                                                     │
                ▼                                          ┌──────────┴──────────┐
    ┌─────────────────────────┐                            │  2. Test locally    │
    │     S3 BUCKETS          │                            │     dbt run         │
    │  ┌───────┐ ┌───────┐    │                            │     dbt test        │
    │  │ Anon  │ │  PII  │    │                            └──────────▲──────────┘
    │  └───────┘ └───────┘    │                                       │
    │  ┌─────────────────┐    │                            ┌──────────┴──────────┐
    │  │    Metrics      │    │                            │  1. Set up local    │
    │  └─────────────────┘    │                            │     DBT environment │
    └─────────────────────────┘                            └─────────────────────┘
                │
                ▼
    You download exports from S3
    based on your access permissions
```

### The Workflow

1. **You** set up a local DBT environment (see [Local Development Setup](#local-development-setup))
2. **You** modify DBT queries or add new metrics
3. **You** test changes locally with the seed database
4. **You** commit and push to GitHub
5. **Cloud server** pulls latest code on next scheduled run
6. **Cloud server** runs DBT and exports to S3
7. **You** access exported data from S3

**Key Point**: GitHub is the mechanism for managing changes. You never modify the production database directly.

---

## How to Add or Modify Metrics

### Option 1: Add a Simple Metric via Catalog

For metrics that can be expressed as a simple SQL aggregation:

1. Edit `dbt/models/metrics/_metrics_catalog.yml`
2. Add your metric definition:

```yaml
- id: new_metric_name
  unit: count                    # or 'percent'
  grain: day
  entity_keys: [organization_id]
  source_model: existing_model   # e.g., patient, encounters
  expression: "count(distinct some_field)"
  description: "What this metric measures"
  version: v1
```

3. Test locally:
```bash
cd dbt
source .envrc
uv run dbt run --profiles-dir . --select fct_metrics_long
```

4. Commit and push to GitHub

### Option 2: Create a Complex Metric Model

For metrics requiring custom SQL logic:

1. Create `dbt/models/metrics/my_new_metric.sql`:

```sql
{{
    config(
        materialized='view'
    )
}}

with source_data as (
    select
        some_date::date as period_date,
        organization_id,
        some_field
    from {{ ref('source_model') }}
    where some_condition
)

select
    period_date,
    organization_id,
    count(distinct some_field) as value
from source_data
group by 1, 2
```

2. Add to catalog if it should appear in `fct_metrics_long`

3. Test locally:
```bash
uv run dbt run --profiles-dir . --select my_new_metric
```

4. Commit and push to GitHub

---

## Local Development Setup

### Prerequisites

- **Docker** (for PostgreSQL)
- **uv** (Python package manager): https://docs.astral.sh/uv/
- **Git** (for version control)

### Step 1: Clone the Repository

```bash
git clone <repository-url>
cd engage-analytics
```

### Step 2: Start PostgreSQL

```bash
docker compose up -d
```

This starts a PostgreSQL container:
- **Host**: localhost
- **Port**: 5434
- **Database**: airbyte
- **User**: postgres
- **Password**: engage

### Step 3: Load Seed Data

```bash
docker exec -i nycdb psql -U postgres -d airbyte < dbt/db/airbyte_schema.sql
```

Verify the data loaded:
```bash
docker exec -it nycdb psql -U postgres -d airbyte -c "SELECT count(*) FROM airbyte.patient;"
```

Expected output: `400` patients

### Step 4: Set Up DBT Environment

```bash
cd dbt
source .envrc
```

Or manually set environment variables:
```bash
export DBT_HOST=localhost
export DBT_PORT=5434
export DBT_USER=postgres
export DBT_PASSWORD=engage
export DBT_DATABASE=airbyte
export DBT_SCHEMA=engage_analytics
```

### Step 5: Install DBT Dependencies

```bash
uv tool install dbt-core --with dbt-postgres
dbt deps --profiles-dir .
```

### Step 6: Load Seed Data and Run Models

```bash
# Load questionnaire metadata
uv run dbt seed --profiles-dir .

# Build all models
uv run dbt run --profiles-dir .
```

### Step 7: Verify

```bash
# Check tables were created
docker exec -it nycdb psql -U postgres -d airbyte -c "\dt engage_analytics_mart.*"

# Generate and view documentation
uv run dbt docs generate --profiles-dir .
uv run dbt docs serve --profiles-dir .
# Open http://localhost:8000
```

### Common DBT Commands

```bash
# Run all models
uv run dbt run --profiles-dir .

# Run specific model
uv run dbt run --profiles-dir . --select patient

# Run model + all dependencies
uv run dbt run --profiles-dir . --select +patient

# Run only metrics
uv run dbt run --profiles-dir . --select "metrics.*"

# Run tests
uv run dbt test --profiles-dir .

# Compile SQL without running
uv run dbt compile --profiles-dir . --select some_model
```

---

## S3 Access & Permissions

Exports are stored in three separate S3 buckets with different access levels.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           S3 BUCKET STRUCTURE                                    │
└─────────────────────────────────────────────────────────────────────────────────┘

    ┌─────────────────────────────────────────────────────────────────────────────┐
    │  engage-analytics-exports-anon/                                             │
    │  ─────────────────────────────                                              │
    │                                                                             │
    │  ACCESS: Broader access for general analysis                                │
    │                                                                             │
    │  CONTENTS:                                                                  │
    │  • patient_anon.csv         (de-identified patient data)                    │
    │  • qr_*_anon.csv            (anonymized questionnaire responses)            │
    │  • encounters.csv           (encounter data)                                │
    │  • practitioners.csv        (provider data)                                 │
    │  • organizations.csv        (organization data)                             │
    │                                                                             │
    │  STRUCTURE: s3://bucket/YYYY/MM/DD/engage_analytics_export_anon_*.zip       │
    └─────────────────────────────────────────────────────────────────────────────┘

    ┌─────────────────────────────────────────────────────────────────────────────┐
    │  engage-analytics-exports-pii/                                              │
    │  ────────────────────────────                                               │
    │                                                                             │
    │  ACCESS: Restricted - requires explicit authorization                       │
    │                                                                             │
    │  CONTENTS:                                                                  │
    │  • patient.csv              (full patient data with PII)                    │
    │  • qr_*.csv                 (full questionnaire responses)                  │
    │  • encounters.csv           (encounter data)                                │
    │  • practitioners.csv        (provider data)                                 │
    │                                                                             │
    │  STRUCTURE: s3://bucket/YYYY/MM/DD/engage_analytics_export_pii_*.zip        │
    └─────────────────────────────────────────────────────────────────────────────┘

    ┌─────────────────────────────────────────────────────────────────────────────┐
    │  engage-analytics-exports-metrics/                                          │
    │  ────────────────────────────────                                           │
    │                                                                             │
    │  ACCESS: Reporting teams and dashboards                                     │
    │                                                                             │
    │  CONTENTS:                                                                  │
    │  • fct_metrics_long.csv     (all metrics in long format)                    │
    │                                                                             │
    │  STRUCTURE: s3://bucket/YYYY/MM/DD/engage_analytics_export_metrics_*.zip    │
    └─────────────────────────────────────────────────────────────────────────────┘
```

### Access Control

- **Anon Bucket**: Safe for general analysis. Contains no personally identifiable information.
- **PII Bucket**: Contains sensitive data. Access requires authorization and should be limited to those with a legitimate need.
- **Metrics Bucket**: Aggregated data only. Safe for dashboards and reporting.

Access is controlled via AWS IAM policies. Contact your administrator to request access to specific buckets.

### Downloading Exports

Using AWS CLI:
```bash
# List available exports
aws s3 ls s3://engage-analytics-exports-anon/2025/01/

# Download latest export
aws s3 cp s3://engage-analytics-exports-anon/2025/01/13/engage_analytics_export_anon_20250113_120000.zip .

# Unzip
unzip engage_analytics_export_anon_20250113_120000.zip
```

---

## Reference

### Key File Locations

| Path | Description |
|------|-------------|
| `dbt/models/staging/` | Raw FHIR extraction (11 models) |
| `dbt/models/intermediate/` | Transform logic (3 models) |
| `dbt/models/marts/resources/` | Final resource tables (patient, encounters, etc.) |
| `dbt/models/marts/qr_named/` | Questionnaire responses with PII (36 models) |
| `dbt/models/marts/qr_anon/` | Anonymized questionnaire responses (36 models) |
| `dbt/models/metrics/` | Aggregate metrics (14 models) |
| `dbt/models/metrics/_metrics_catalog.yml` | Metric definitions |
| `dbt/seeds/questionnaire_metadata.csv` | Column name mappings for questionnaires |
| `dbt/macros/` | Reusable SQL macros |
| `dataexport/` | Export scripts for S3 |

### Seed Data Summary

The local development database contains:

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

### Common Commands Cheatsheet

```bash
# ─── Docker ───────────────────────────────────────────
docker compose up -d              # Start PostgreSQL
docker compose down               # Stop PostgreSQL
docker compose logs nycdb         # View logs

# ─── DBT ──────────────────────────────────────────────
cd dbt && source .envrc           # Set environment

uv run dbt seed --profiles-dir .  # Load seed data
uv run dbt run --profiles-dir .   # Run all models
uv run dbt test --profiles-dir .  # Run tests

uv run dbt run --profiles-dir . --select patient          # Run one model
uv run dbt run --profiles-dir . --select +patient         # Model + deps
uv run dbt run --profiles-dir . --select "metrics.*"      # All metrics

uv run dbt docs generate --profiles-dir .                 # Generate docs
uv run dbt docs serve --profiles-dir .                    # Serve at :8000

# ─── Git ──────────────────────────────────────────────
git status                        # Check changes
git add <file>                    # Stage changes
git commit -m "Add new metric"    # Commit
git push                          # Push to GitHub
```

### Further Documentation

- `dbt/docs/summary.md` - Detailed architecture overview
- `dbt/docs/metrics.md` - Metrics system documentation
- `dbt/docs/metadata-workflow.md` - Questionnaire metadata workflow
- `dataexport/README.md` - Export script documentation
