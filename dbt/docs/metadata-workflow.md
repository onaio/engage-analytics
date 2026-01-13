# Metadata and Model Generation Workflow

This document explains how to manage questionnaire metadata and generate dbt models.

## Overview

Two consolidated scripts handle all metadata and model operations:

- `metadata_manager.py` - Manages questionnaire metadata (labels, PII flags, column names)
- `model_generator.py` - Generates dbt SQL models for questionnaire responses

## Quick Start: Adding a New Questionnaire

When a new questionnaire is added to the system:

```bash
cd /Users/mberg/github/engage-analytics/dbt

# 1. Run full metadata refresh
python metadata_manager.py full-refresh

# 2. Regenerate all models
python model_generator.py all

# 3. Run dbt to create views
uv run dbt run --profiles-dir .
```

## metadata_manager.py Commands

### extract-text
Extracts question text from questionnaire JSON files and creates `linkid_question_mapping.json`.

```bash
python metadata_manager.py extract-text
```

**Input:** Questionnaire JSON files in `/Users/mberg/github/engage-analytics/questionnaire/`
**Output:** `linkid_question_mapping.json` (linkId to question text mapping)

### enrich
Creates the main metadata file with human-readable labels, shortened column names, and PII flags.

```bash
python metadata_manager.py enrich
```

**Requires:**
- `linkid_question_mapping.json` (run `extract-text` first)
- Database connection (reads actual table columns)

**Output:** `seeds/questionnaire_metadata.csv`

### add-common
Adds common fields that appear across multiple questionnaires (DOB, Age, Encounter Reference).

```bash
python metadata_manager.py add-common
```

**Requires:** Database connection
**Modifies:** `seeds/questionnaire_metadata.csv`

### fix-hex
Handles hex-formatted linkIds specific to Questionnaire/55 (IPC Session 1).

```bash
python metadata_manager.py fix-hex
```

**Requires:** Database connection
**Modifies:** `seeds/questionnaire_metadata.csv`

### fix-duplicates
Ensures unique short_name values by adding numeric suffixes to duplicates.

```bash
python metadata_manager.py fix-duplicates
```

**Modifies:** `seeds/questionnaire_metadata.csv`

### find-unmapped
Finds UUID-formatted linkIds in the database that aren't yet in metadata.

```bash
python metadata_manager.py find-unmapped
```

**Requires:** Database connection
**Output:** `unmapped_uuid_fields.csv`

### status
Shows status of all metadata files.

```bash
python metadata_manager.py status
```

### full-refresh
Runs all operations in the correct sequence:

1. `extract-text` - Extract question text
2. `enrich` - Create metadata with labels/PII flags
3. `add-common` - Add common fields
4. `fix-hex` - Handle hex linkIds
5. `fix-duplicates` - Fix duplicate names
6. `find-unmapped` - Report unmapped fields

```bash
python metadata_manager.py full-refresh
```

## model_generator.py Commands

### named
Generates readable questionnaire response models in `models/marts/qr_named/`.

```bash
python model_generator.py named
```

**Input:** `discovered_questionnaires.csv`
**Output:** `models/marts/qr_named/*.sql`

### anon
Generates anonymized models in `models/marts/qr_anon/` that mask PII fields.

```bash
python model_generator.py anon
```

**Input:** `seeds/questionnaire_metadata.csv`
**Output:** `models/marts/qr_anon/*.sql`

### all
Generates both named and anonymized models.

```bash
python model_generator.py all
```

## Key Files

| File | Purpose |
|------|---------|
| `seeds/questionnaire_metadata.csv` | Central metadata: column names, labels, PII flags |
| `linkid_question_mapping.json` | LinkId to question text mapping |
| `discovered_questionnaires.csv` | List of known questionnaires |
| `unmapped_uuid_fields.csv` | UUID fields not yet in metadata |

## Metadata CSV Structure

The `questionnaire_metadata.csv` file has these columns:

| Column | Description |
|--------|-------------|
| `table` | dbt table name (e.g., `qr_registration_info`) |
| `column` | Original column name from the database |
| `linkid` | FHIR linkId (for UUID columns) |
| `short_name` | Shortened column name for readability |
| `label` | Human-readable label |
| `data_type` | PostgreSQL data type |
| `questionnaire_title` | Questionnaire name |
| `source` | `system`, `generated`, or `questionnaire` |
| `anon` | `TRUE` if PII, `FALSE` otherwise |

## PII Handling

Fields are marked as PII (`anon=TRUE`) if they match these patterns:
- `patient-name`, `patient-dob`, `first_name`, `last_name`
- `date_of_birth`, `medicaid_number`, `email_address`
- `physical_address`, `phone_number`, `zip_code`
- `unique_id`, `name_family`, `name_given`, `birthdate`

Anonymized models (`qr_*_anon`) mask these fields:
- Names/IDs: Replaced with `REDACTED`
- Phone numbers: Masked as `XXX-XXX-1234` (last 4 digits)
- Zip codes: Masked as `123XX` (first 3 digits)
- Patient/QR IDs: Hashed with MD5

## Environment Variables

Required for database operations:

```bash
export DBT_HOST=localhost
export DBT_PORT=5432
export DBT_USER=postgres
export DBT_PASSWORD=knox48
export DBT_DATABASE=airbyte
```

Or use the `.envrc` file:

```bash
source .envrc
```

## Troubleshooting

### "linkid_question_mapping.json not found"
Run `extract-text` before `enrich`:
```bash
python metadata_manager.py extract-text
python metadata_manager.py enrich
```

### Database connection errors
Ensure environment variables are set:
```bash
set -a && source .envrc && set +a
```

### dbt models fail after metadata changes
Regenerate models and reload seed:
```bash
python model_generator.py all
uv run dbt seed --profiles-dir .
uv run dbt run --profiles-dir .
```

## Backup Scripts

Original scripts are preserved in `bak/` for reference:
- `extract_questionnaire_text.py`
- `update_metadata_with_questions.py`
- `add_common_fields_to_metadata.py`
- `fix_hex_linkids.py`
- `fix_duplicate_short_names.py`
- And others...
