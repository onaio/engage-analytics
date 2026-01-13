# Engage Analytics dbt Project

This document explains how the dbt project transforms raw FHIR healthcare data into analytical models with built-in PII anonymization.

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Data Flow](#data-flow)
4. [Project Structure](#project-structure)
5. [Staging Layer](#staging-layer)
6. [Intermediate Layer](#intermediate-layer)
7. [Marts Layer](#marts-layer)
8. [Macros](#macros)
9. [Seeds (Reference Data)](#seeds-reference-data)
10. [Anonymization Pattern](#anonymization-pattern)
11. [Questionnaire Response Pattern](#questionnaire-response-pattern)
12. [Adding a New Questionnaire](#adding-a-new-questionnaire)
13. [Running dbt](#running-dbt)

---

## Overview

This dbt project transforms FHIR (Fast Healthcare Interoperability Resources) data from Airbyte into analytical models. It implements a medallion architecture:

```
Raw Data (Airbyte) → Staging → Intermediate → Marts
```

**Key Features:**
- Transforms nested FHIR JSON into flat, queryable tables
- Automatically creates anonymized versions of all questionnaire data
- Uses metadata-driven column naming (no hardcoded field names)
- Incremental processing for efficiency

---

## Architecture

### Database Configuration

| Setting | Value |
|---------|-------|
| Database | PostgreSQL |
| Source Schema | `airbyte` (raw FHIR data) |
| Staging Schema | `engage_analytics_engage_analytics_stg` |
| Intermediate Schema | `engage_analytics_engage_analytics_int` |
| Marts Schema | `engage_analytics_engage_analytics_mart` |

### Environment Variables

```bash
DBT_HOST=localhost
DBT_PORT=5432
DBT_USER=postgres
DBT_PASSWORD=<password>
DBT_DATABASE=airbyte
DBT_SCHEMA=engage_analytics
```

---

## Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                         RAW DATA (Airbyte)                       │
│  patient, encounter, practitioner, questionnaire_response, etc. │
└─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                      STAGING LAYER (stg_*)                       │
│  Extract FHIR JSON fields, filter completed responses            │
│  Examples: stg_patient, stg_encounter, stg_questionnaire_response│
└─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                   INTERMEDIATE LAYER (int_*)                     │
│  Flatten nested structures, pivot metadata tags                  │
│  - int_qr_answers_long: Flatten questionnaire items              │
│  - int_qr_header: Extract QR metadata                            │
│  - int_qr_tags: Pivot encounter tags to columns                  │
└─────────────────────────────────────────────────────────────────┘
                                 │
                    ┌────────────┴────────────┐
                    ▼                         ▼
┌───────────────────────────┐   ┌───────────────────────────┐
│     RESOURCE MARTS        │   │   QUESTIONNAIRE MARTS     │
│  patient, encounters,     │   │                           │
│  practitioners, orgs      │   │  ┌─────────────────────┐  │
│                           │   │  │  qr_* (with PII)    │  │
│  + patient_anon           │   │  │  Readable columns   │  │
│    (anonymized patient)   │   │  └─────────────────────┘  │
│                           │   │            │              │
└───────────────────────────┘   │            ▼              │
                                │  ┌─────────────────────┐  │
                                │  │  qr_*_anon          │  │
                                │  │  PII masked/hashed  │  │
                                │  └─────────────────────┘  │
                                └───────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                         METRICS LAYER                            │
│  active_providers, clients_eligible_*, fct_metrics_long          │
└─────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
dbt/
├── dbt_project.yml          # Project configuration
├── profiles.yml             # Database connection
├── packages.yml             # Dependencies (dbt-utils)
│
├── metadata_manager.py      # Metadata operations (extract, enrich, fix)
├── model_generator.py       # Generate dbt model files
│
├── models/
│   ├── sources/
│   │   └── engage_dataset.yml    # Raw source definitions
│   │
│   ├── staging/                  # Extract from raw FHIR JSON
│   │   ├── stg_patient.sql
│   │   ├── stg_encounter.sql
│   │   ├── stg_practitioner.sql
│   │   ├── stg_questionnaire_response.sql
│   │   └── ... (11 staging models)
│   │
│   ├── intermediate/             # Transform and flatten
│   │   ├── int_qr_answers_long.sql
│   │   ├── int_qr_header.sql
│   │   └── int_qr_tags.sql
│   │
│   ├── marts/
│   │   ├── resources/            # Dimension tables
│   │   │   ├── patient.sql
│   │   │   ├── patient_anon.sql
│   │   │   ├── encounters.sql
│   │   │   ├── practitioners.sql
│   │   │   └── ... (8 resource models)
│   │   │
│   │   ├── qr_named/             # Questionnaires with readable names
│   │   │   ├── qr_registration_info.sql
│   │   │   ├── qr_gad_7_s1.sql
│   │   │   └── ... (36 questionnaire models)
│   │   │
│   │   └── qr_anon/              # Anonymized questionnaires
│   │       ├── qr_registration_info_anon.sql
│   │       ├── qr_gad_7_s1_anon.sql
│   │       └── ... (35 anonymized models)
│   │
│   └── metrics/                  # Aggregated KPIs
│       ├── active_providers.sql
│       ├── clients_eligible_for_ipc.sql
│       ├── fct_metrics_long.sql
│       └── ... (10+ metric models)
│
├── macros/                       # Reusable SQL templates
│   ├── build_qr_wide_readable.sql
│   ├── create_anonymized_qr_view.sql
│   ├── meta_tag_pivot.sql
│   └── ... (10 macros)
│
└── seeds/
    └── questionnaire_metadata.csv   # Column mapping & PII flags
```

---

## Staging Layer

Staging models extract data from raw FHIR JSON. All are **incremental** models that track `_airbyte_emitted_at` for change detection.

### Key Staging Models

| Model | Purpose |
|-------|---------|
| `stg_questionnaire_response` | Extracts completed questionnaire responses with nested item arrays |
| `stg_patient` | Patient demographics (name, phone, birthDate, gender) |
| `stg_encounter` | Clinical encounters/visits |
| `stg_practitioner` | Healthcare provider information |
| `stg_practitioner_roles` | Provider roles and organization affiliations |
| `stg_organizations` | Healthcare organization data |
| `stg_locations` | Physical location data |
| `stg_careteams` | Care team membership |

### Example: stg_patient

Extracts patient data from FHIR JSON:

```sql
select
  resource::jsonb ->> 'id' as id,
  resource::jsonb ->> 'gender' as gender,
  resource::jsonb ->> 'birthDate' as birthDate,
  resource::jsonb -> 'name' -> 0 ->> 'given' as name_given,
  resource::jsonb -> 'name' -> 0 ->> 'family' as name_family,
  resource::jsonb -> 'telecom' -> 0 ->> 'value' as phone_number
from {{ source('engage', 'patient') }}
```

---

## Intermediate Layer

Intermediate models transform staging data into formats ready for marts.

### int_qr_answers_long

The most complex model. Uses recursive CTEs to flatten nested questionnaire items:

**Input (nested JSON):**
```json
{
  "item": [
    { "linkId": "name", "answer": [{ "valueString": "John" }] },
    { "linkId": "section1", "item": [
        { "linkId": "q1", "answer": [{ "valueInteger": 5 }] }
    ]}
  ]
}
```

**Output (flat table):**
| qr_id | linkid | answer_value_text |
|-------|--------|-------------------|
| qr-1 | name | John |
| qr-1 | q1 | 5 |

### int_qr_header

Extracts questionnaire response metadata:
- `qr_id` - Response ID
- `questionnaire_id` - Which form (e.g., "Questionnaire/221")
- `subject_patient_id` - Patient reference
- `encounter_id` - Encounter reference
- `author_practitioner_id` - Who completed it

### int_qr_tags

Pivots FHIR meta.tag arrays into columns:
- `practitioner_organization_id`
- `practitioner_id`
- `practitioner_location_id`
- `practitioner_careteam_id`

---

## Marts Layer

### Resource Models

Core dimensional tables for FHIR resources:

| Model | Key Fields |
|-------|------------|
| `patient` | id, gender, birth_date, age_years, name, phone_number, organization_id |
| `patient_anon` | Same as patient but with PII masked/hashed |
| `encounters` | id, class, period_start/end, service_type, practitioner_id |
| `practitioners` | id, name, phone, role, organization_id, location_id |
| `organizations` | id, name, type, active |
| `current_practitioner_role` | Latest role per practitioner |

### Questionnaire Models

Each questionnaire has two versions:

1. **Named version** (`qr_*.sql`) - Readable column names, contains PII
2. **Anonymized version** (`qr_*_anon.sql`) - PII masked, IDs hashed

**Example questionnaires:**
- `qr_registration_info` - Patient registration form
- `qr_gad_7_s1` through `qr_gad_7_s4` - GAD-7 anxiety scale (4 sessions)
- `qr_phq_9_s1` through `qr_phq_9_s4` - PHQ-9 depression scale
- `qr_start_ipc_s1` through `qr_start_ipc_s4` - IPC session tracking
- `qr_sbirt` - Substance abuse screening

### Metrics Models

Aggregated KPIs for reporting:

| Model | Purpose |
|-------|---------|
| `active_providers` | Providers with activity in past 30 days |
| `clients_eligible_for_ipc` | Clients eligible for IPC intervention |
| `clients_accepted_ipc` | Clients who started IPC |
| `encounters_by_delivery_format` | Encounters by delivery method |
| `fct_metrics_long` | All metrics in one fact table |

---

## Macros

Macros are reusable SQL templates that power the transformation logic.

### build_qr_wide_readable

**Purpose:** Transforms questionnaire answers from long format to wide format with readable column names.

**How it works:**
1. Joins `int_qr_answers_long` with `questionnaire_metadata` to get column names
2. Pivots linkid values to columns
3. Aggregates multiple answers with pipe separator
4. Adds header and tag columns

**Usage:**
```sql
{{ build_qr_wide_readable(['Questionnaire/221'], 'qr_registration_info') }}
```

### create_anonymized_qr_view

**Purpose:** Creates anonymized version of a questionnaire view.

**How it works:**
1. Queries `questionnaire_metadata` for fields where `anon = 'TRUE'`
2. Applies type-aware masking:
   - Phone → `XXX-XXX-` + last 4 digits
   - Email/Name/Address → `REDACTED`
   - Zip → First 3 digits + `XX`
   - SSN → `XXX-XX-` + last 4 digits
3. Hashes `qr_id` and `subject_patient_id` with MD5

**Usage:**
```sql
{{ create_anonymized_qr_view('qr_registration_info', []) }}
```

### meta_tag_pivot

**Purpose:** Pivots FHIR meta.tag arrays into columns.

**How it works:**
1. Unnests `meta::jsonb -> 'tag'` array
2. Maps tag displays to standardized names
3. Uses dbt_utils.pivot() to create columns

### Other Macros

| Macro | Purpose |
|-------|---------|
| `resource_level_1_extraction` | Extract top-level FHIR JSON fields |
| `resource_level_2_extraction` | Extract nested JSON objects dynamically |
| `mask_pii_fields` | Query metadata for PII field list |
| `find_resource_id` | Parse "Patient/123" → "123" |
| `find_resource_name` | Parse "Patient/123" → "Patient" |

---

## Seeds (Reference Data)

### questionnaire_metadata.csv

This CSV controls how questionnaire columns are named and which fields need anonymization.

**Key Columns:**

| Column | Purpose |
|--------|---------|
| `table` | Target mart table name (e.g., 'qr_registration_info') |
| `column` | Output column name in the mart |
| `linkid` | FHIR question identifier |
| `short_name` | Abbreviated name |
| `label` | Human-readable question text |
| `questionnaire_title` | Form name |
| `anon` | TRUE if field contains PII |

**Example rows:**
```csv
table,column,linkid,short_name,label,anon
qr_registration_info,first_name,1.1,fname,First Name,TRUE
qr_registration_info,anxiety_score,2.1,gad_score,GAD-7 Total,FALSE
```

**Why this matters:**
- Column names come from metadata, not hardcoded SQL
- PII flagging is centralized in one place
- Non-developers can update naming without touching code

---

## Anonymization Pattern

The system implements metadata-driven anonymization at two levels:

### 1. Patient Level (`patient_anon.sql`)

| Original Field | Anonymized Version |
|----------------|-------------------|
| `id` | MD5 hash |
| `birth_date` | Replaced with `age_years` |
| `phone_number` | `XXX-XXX-` + last 4 digits |
| `name_family`, `name_given` | `REDACTED` |

### 2. Questionnaire Level (`create_anonymized_qr_view`)

Reads `questionnaire_metadata.anon = 'TRUE'` and applies:

| Data Type | Masking Rule |
|-----------|--------------|
| Phone numbers | `XXX-XXX-` + last 4 digits |
| Email addresses | `REDACTED` |
| Physical addresses | `REDACTED` |
| Zip codes | First 3 digits + `XX` |
| Names/identifiers | `REDACTED` |
| Medicaid numbers | `REDACTED` |
| SSN | `XXX-XX-` + last 4 digits |

**IDs are hashed:** `qr_id` and `subject_patient_id` use MD5 hashing to maintain referential integrity without exposing original values.

---

## Questionnaire Response Pattern

FHIR QuestionnaireResponse resources are complex nested JSON. Here's how they're transformed:

### Raw FHIR Structure

```json
{
  "id": "qr-uuid",
  "questionnaire": "Questionnaire/221",
  "subject": { "reference": "Patient/123" },
  "author": { "reference": "Practitioner/789" },
  "item": [
    {
      "linkId": "first-name",
      "answer": [{ "valueString": "John" }]
    },
    {
      "linkId": "section1",
      "item": [
        {
          "linkId": "q1",
          "answer": [{ "valueInteger": 5 }]
        }
      ]
    }
  ]
}
```

### Transformation Pipeline

```
stg_questionnaire_response
        │
        │ Extract: id, questionnaire_id, subject, items array
        │ Filter: status = 'completed'
        ▼
int_qr_answers_long
        │
        │ Recursive CTE flattens nested items
        │ Output: qr_id, linkid, answer_value_text
        ▼
int_qr_header + int_qr_tags
        │
        │ Add metadata columns
        ▼
build_qr_wide_readable (macro)
        │
        │ Join with questionnaire_metadata for column names
        │ Pivot long → wide
        ▼
qr_registration_info (named view)
        │
        │ One row per response
        │ Columns: first_name, last_name, phone, etc.
        ▼
create_anonymized_qr_view (macro)
        │
        │ Apply PII masking per metadata
        ▼
qr_registration_info_anon (anonymized view)
```

---

## Adding a New Questionnaire

When a new questionnaire is added to the system, use the consolidated Python scripts to update metadata and regenerate models.

### Quick Start

```bash
cd /Users/mberg/github/engage-analytics/dbt

# 1. Refresh all metadata (extracts question text, enriches with labels/PII flags)
python metadata_manager.py full-refresh

# 2. Regenerate all model files
python model_generator.py all

# 3. Run dbt to create the views
uv run dbt seed --profiles-dir .
uv run dbt run --profiles-dir .
```

### What the Scripts Do

**`metadata_manager.py full-refresh`** runs these steps in sequence:
1. `extract-text` - Extract question text from questionnaire JSON files
2. `enrich` - Create metadata CSV with labels, short names, and PII flags
3. `add-common` - Add common fields (DOB, Age, Encounter Reference)
4. `fix-hex` - Handle hex-formatted linkIds
5. `fix-duplicates` - Ensure unique column names
6. `find-unmapped` - Report any unmapped UUID fields

**`model_generator.py all`** generates:
- Named models in `models/marts/qr_named/` (readable column names, contains PII)
- Anonymized models in `models/marts/qr_anon/` (PII masked/hashed)

### Manual Steps (if needed)

If you need to manually add a questionnaire:

1. Add rows to `seeds/questionnaire_metadata.csv` with appropriate `table`, `column`, `linkid`, `short_name`, `label`, and `anon` values
2. Add entry to `discovered_questionnaires.csv`
3. Run `python model_generator.py all` to regenerate models
4. Run `uv run dbt seed --profiles-dir . && uv run dbt run --profiles-dir .`

For detailed command reference, see `docs/metadata-workflow.md`.

---

## Running dbt

### Prerequisites

```bash
cd /Users/mberg/github/engage-analytics/dbt
source .envrc  # or set environment variables manually
```

### Common Commands

```bash
# Run all models
uv run dbt run --profiles-dir .

# Run specific model
uv run dbt run --profiles-dir . --select patient

# Run model + its upstream dependencies
uv run dbt run --profiles-dir . --select +patient

# Run model + downstream dependents
uv run dbt run --profiles-dir . --select patient+

# Run only questionnaire models
uv run dbt run --profiles-dir . --select "qr_*"

# Reload seed data (questionnaire_metadata.csv)
uv run dbt seed --profiles-dir .

# Full rebuild (seed + run)
uv run dbt seed --profiles-dir . && uv run dbt run --profiles-dir .

# Test models
uv run dbt test --profiles-dir .

# Generate documentation
uv run dbt docs generate --profiles-dir .
uv run dbt docs serve --profiles-dir .
```

The `--profiles-dir .` tells dbt to use the local `profiles.yml` instead of looking in `~/.dbt/`.

### Using the Wrapper Script

The dataexport project includes a wrapper that runs dbt before exporting:

```bash
cd /Users/mberg/github/engage-analytics/dataexport
./run_export.sh anon    # Refresh dbt + export anonymized data
./run_export.sh pii     # Refresh dbt + export PII data
./run_export.sh both    # Refresh dbt + export both
```

---

## Summary

This dbt project:

1. **Ingests** FHIR healthcare data from Airbyte
2. **Extracts** nested JSON into flat staging tables
3. **Transforms** questionnaire responses from nested items to wide tables with readable column names
4. **Anonymizes** PII using metadata-driven masking rules
5. **Aggregates** metrics for reporting

The key design principles are:
- **Metadata-driven:** Column names and PII flags live in CSV, not code
- **Incremental:** Only process new/changed records
- **Dual delivery:** Every questionnaire has both PII and anonymized versions
- **Macro-powered:** Complex logic is encapsulated in reusable macros
