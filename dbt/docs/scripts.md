# Python Scripts Reference

This document describes the Python utility scripts in the dbt project, their purpose, dependencies, and usage status.

## Table of Contents

1. [Overview](#overview)
2. [Script Categories](#script-categories)
3. [Recommended Workflow](#recommended-workflow)
4. [Script Details](#script-details)
5. [Status Summary](#status-summary)
6. [Candidates for Removal](#candidates-for-removal)

---

## Overview

The dbt directory contains 16 Python scripts that support:

- **Metadata extraction** - Pulling question text and linkIds from questionnaire JSON
- **Metadata management** - Fixing, enriching, and normalizing the questionnaire_metadata.csv
- **Model generation** - Creating dbt SQL models for questionnaire views
- **Data export** - Exporting mart data to CSV

The central artifact these scripts manage is `seeds/questionnaire_metadata.csv`, which controls column naming and PII anonymization.

---

## Script Categories

```
┌─────────────────────────────────────────────────────────────────┐
│                    METADATA EXTRACTION                          │
│  extract_questionnaire_text.py → linkid_question_mapping.json   │
│  extract_questionnaire_metadata.py                              │
│  find_all_uuid_fields.py → unmapped_uuid_fields.csv             │
│  extract_uuid_metadata.py                                       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    METADATA FIXING                              │
│  add_common_fields_to_metadata.py                               │
│  fix_duplicate_short_names.py                                   │
│  fix_hex_linkids.py                                             │
│  update_hex_aliases.py                                          │
│  add_linkid_column.py (likely obsolete)                         │
│  fix_metadata_columns.py (likely obsolete)                      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    METADATA ENRICHMENT                          │
│  update_metadata_with_questions.py                              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    MODEL GENERATION                             │
│  generate_named_readable_models.py → models/marts/qr_named/     │
│  generate_anon_models_v2.py → models/marts/qr_anon/             │
│  generate_readable_models.py (legacy)                           │
│  generate_anon_models.py (replaced by v2)                       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    DATA EXPORT                                  │
│  export_marts_to_csv.py → dbt/data/*.csv                        │
└─────────────────────────────────────────────────────────────────┘
```

---

## Recommended Workflow

When adding a new questionnaire or refreshing metadata:

### Step 1: Extract Question Text
```bash
python extract_questionnaire_text.py
# Output: linkid_question_mapping.json
```

### Step 2: Find Unmapped UUID Fields
```bash
python find_all_uuid_fields.py
# Output: unmapped_uuid_fields.csv
```

### Step 3: Extract UUID Metadata
```bash
python extract_uuid_metadata.py
# Updates: questionnaire_metadata.csv
```

### Step 4: Add Common Fields
```bash
python add_common_fields_to_metadata.py
# Updates: questionnaire_metadata.csv
```

### Step 5: Fix Duplicates and Hex IDs
```bash
python fix_duplicate_short_names.py
python fix_hex_linkids.py
python update_hex_aliases.py
# Updates: questionnaire_metadata.csv
```

### Step 6: Enrich with Question Labels
```bash
python update_metadata_with_questions.py
# Updates: questionnaire_metadata.csv (adds labels, short_names, anon flags)
```

### Step 7: Generate Models
```bash
python generate_named_readable_models.py
# Output: models/marts/qr_named/*.sql

python generate_anon_models_v2.py
# Output: models/marts/qr_anon/*.sql
```

### Step 8: Load Seed and Run dbt
```bash
uv run dbt seed --profiles-dir .
uv run dbt run --profiles-dir .
```

---

## Script Details

### Metadata Extraction Scripts

#### extract_questionnaire_text.py

| Attribute | Value |
|-----------|-------|
| **Purpose** | Extract linkId → question text mappings from questionnaire JSON files |
| **Input** | Questionnaire JSON files (recursive search) |
| **Output** | `linkid_question_mapping.json` (301KB) |
| **Status** | ✅ ACTIVE |

Creates the reference mapping used by other scripts to get human-readable question text for each linkId.

#### extract_questionnaire_metadata.py

| Attribute | Value |
|-----------|-------|
| **Purpose** | Extract initial metadata from questionnaire definitions |
| **Input** | `discovered_questionnaires.csv`, questionnaire JSON files |
| **Output** | `questionnaire_metadata_full.csv` |
| **Status** | ✅ ACTIVE |

Populates initial metadata with questionnaire_id, linkid, question_text, and question_order.

#### find_all_uuid_fields.py

| Attribute | Value |
|-----------|-------|
| **Purpose** | Find UUID-formatted linkIds not yet in metadata |
| **Input** | Database table `int_qr_answers_long` |
| **Output** | `unmapped_uuid_fields.csv` (11KB) |
| **Status** | ✅ ACTIVE |

Scans the database for UUID linkIds that haven't been mapped yet. Provides input for `extract_uuid_metadata.py`.

#### extract_uuid_metadata.py

| Attribute | Value |
|-----------|-------|
| **Purpose** | Add UUID-formatted linkIds to metadata with generated aliases |
| **Input** | `unmapped_uuid_fields.csv`, `questionnaire_metadata.csv` |
| **Output** | Updated `questionnaire_metadata.csv` |
| **Status** | ✅ ACTIVE |

Handles the special case of UUID linkIds that aren't human-readable.

---

### Metadata Fixing Scripts

#### add_common_fields_to_metadata.py

| Attribute | Value |
|-----------|-------|
| **Purpose** | Add metadata entries for common fields (DOB, Age, Encounter Reference) |
| **Input** | `questionnaire_metadata.csv`, database |
| **Output** | Updated `questionnaire_metadata.csv` |
| **Status** | ✅ ACTIVE |

Adds system/calculated fields that appear across multiple questionnaires.

#### fix_duplicate_short_names.py

| Attribute | Value |
|-----------|-------|
| **Purpose** | Ensure unique short_name values by adding numeric suffixes |
| **Input** | `questionnaire_metadata.csv` |
| **Output** | Updated `questionnaire_metadata.csv` |
| **Status** | ✅ ACTIVE |

PostgreSQL requires unique column names. This script fixes duplicates like `field` → `field`, `field_2`, `field_3`.

#### fix_hex_linkids.py

| Attribute | Value |
|-----------|-------|
| **Purpose** | Handle hex-formatted linkIds in Questionnaire/55 |
| **Input** | Database, `questionnaire_metadata.csv` |
| **Output** | Updated `questionnaire_metadata.csv` |
| **Status** | ✅ ACTIVE |

Questionnaire/55 uses hex linkIds instead of standard format. This creates readable aliases.

#### update_hex_aliases.py

| Attribute | Value |
|-----------|-------|
| **Purpose** | Improve hex linkId aliases using sample values |
| **Input** | Database, `questionnaire_metadata.csv` |
| **Output** | Updated `questionnaire_metadata.csv` |
| **Status** | ✅ ACTIVE |

Enhancement to `fix_hex_linkids.py` that creates better aliases based on actual data values.

#### add_linkid_column.py

| Attribute | Value |
|-----------|-------|
| **Purpose** | Map linkIds to questionnaire response columns |
| **Input** | `questionnaire_metadata.csv`, database, dbt model files |
| **Output** | Updated `questionnaire_metadata.csv` |
| **Status** | ⚠️ LIKELY OBSOLETE |

This functionality appears to be handled by later scripts in the pipeline.

#### fix_metadata_columns.py

| Attribute | Value |
|-----------|-------|
| **Purpose** | Swap column/short_name values to align UUID columns |
| **Input** | `questionnaire_metadata.csv` |
| **Output** | Updated `questionnaire_metadata.csv` |
| **Status** | ⚠️ LIKELY OBSOLETE |

Fixes a data structure issue that seems resolved in v2 scripts.

---

### Metadata Enrichment Scripts

#### update_metadata_with_questions.py

| Attribute | Value |
|-----------|-------|
| **Purpose** | Enrich metadata with human-readable labels, short names, and PII flags |
| **Input** | `linkid_question_mapping.json`, `discovered_questionnaires.csv`, database |
| **Output** | Updated `questionnaire_metadata.csv` |
| **Status** | ✅ ACTIVE - Core Script |

This is the most comprehensive metadata script. It:
- Adds human-readable labels from question text
- Creates shortened column names
- Sets PII flags for anonymization
- Classifies sources (system vs questionnaire)
- Adds questionnaire titles

**This is likely the "final" script in the metadata pipeline.**

---

### Model Generation Scripts

#### generate_named_readable_models.py

| Attribute | Value |
|-----------|-------|
| **Purpose** | Generate dbt models with semantic naming |
| **Input** | `discovered_questionnaires.csv` |
| **Output** | `models/marts/qr_named/*.sql` (36 files), `_model_mappings.md` |
| **Status** | ✅ ACTIVE |

Uses discovery-driven approach. More maintainable than hard-coded version.

**Generated model template:**
```sql
{{
    config(
        materialized='view'
    )
}}

{{ build_qr_wide_readable(['Questionnaire/221'], 'qr_registration_info') }}
```

#### generate_anon_models_v2.py

| Attribute | Value |
|-----------|-------|
| **Purpose** | Generate anonymization views with database introspection |
| **Input** | `questionnaire_metadata.csv`, database |
| **Output** | `models/marts/qr_anon/*.sql` (35 files) |
| **Status** | ✅ ACTIVE - Current Version |

Improved version that queries actual database columns. Handles dynamic column naming better than v1.

**Generated model template:**
```sql
{{
    config(
        materialized='view'
    )
}}

{{ create_anonymized_qr_view('qr_registration_info', []) }}
```

#### generate_readable_models.py

| Attribute | Value |
|-----------|-------|
| **Purpose** | Generate readable models with hard-coded questionnaire list |
| **Input** | Hard-coded list of 30 questionnaires |
| **Output** | `models/marts/qr_readable/*.sql` |
| **Status** | ⚠️ LEGACY |

Uses hard-coded questionnaire IDs. `generate_named_readable_models.py` is the newer, discovery-driven approach.

#### generate_anon_models.py

| Attribute | Value |
|-----------|-------|
| **Purpose** | Generate anonymization views (original version) |
| **Input** | `questionnaire_metadata.csv` |
| **Output** | `models/marts/qr_anon/*.sql` |
| **Status** | ❌ REPLACED BY v2 |

v2 is more robust and handles edge cases better.

---

### Data Export Scripts

#### export_marts_to_csv.py

| Attribute | Value |
|-----------|-------|
| **Purpose** | Export questionnaire mart views to CSV files |
| **Input** | Database mart views |
| **Output** | `dbt/data/*.csv` |
| **Status** | ❓ UNCERTAIN |

Data export utility. May have been replaced by `dataexport/export_to_s3.py`.

---

## Status Summary

### ✅ Active Scripts (12)

| Script | Purpose |
|--------|---------|
| `extract_questionnaire_text.py` | Extract question text mapping |
| `extract_questionnaire_metadata.py` | Initial metadata extraction |
| `find_all_uuid_fields.py` | Find unmapped UUID fields |
| `extract_uuid_metadata.py` | Map UUID fields |
| `add_common_fields_to_metadata.py` | Add demographic fields |
| `fix_duplicate_short_names.py` | Ensure unique column names |
| `fix_hex_linkids.py` | Handle hex linkId format |
| `update_hex_aliases.py` | Improve hex aliases |
| `update_metadata_with_questions.py` | Enrich metadata (core script) |
| `generate_named_readable_models.py` | Generate qr_named models |
| `generate_anon_models_v2.py` | Generate qr_anon models |
| `generate_readable_models.py` | Generate legacy readable models |

### ⚠️ Likely Obsolete (3)

| Script | Reason |
|--------|--------|
| `add_linkid_column.py` | Functionality handled by later scripts |
| `fix_metadata_columns.py` | Data structure issue resolved in v2 |
| `generate_anon_models.py` | Replaced by v2 |

### ❓ Uncertain (1)

| Script | Reason |
|--------|--------|
| `export_marts_to_csv.py` | May be replaced by dataexport scripts |

---

## Candidates for Removal

The following scripts are candidates for removal after verification:

### 1. generate_anon_models.py
**Reason:** `generate_anon_models_v2.py` is the current implementation.
**Risk:** Low - v2 is clearly the active version.
**Verification:** Confirm v2 generates all 35 anon models correctly.

### 2. add_linkid_column.py
**Reason:** Appears to be an early-stage script whose functionality is now handled by `update_metadata_with_questions.py`.
**Risk:** Medium - need to verify all linkId mappings are correct without it.
**Verification:** Check if running the current pipeline produces complete metadata.

### 3. fix_metadata_columns.py
**Reason:** Handles a column/short_name swap issue that v2 scripts handle correctly.
**Risk:** Medium - need to verify metadata structure is correct.
**Verification:** Check that questionnaire_metadata.csv has correct column structure after running current pipeline.

### 4. export_marts_to_csv.py
**Reason:** The `dataexport/export_to_s3.py` script provides more robust export functionality with S3 support.
**Risk:** Low - unless this is used for local debugging.
**Verification:** Confirm dataexport scripts meet all export needs.

---

## Notes

### Database Credentials
Several scripts have hard-coded database credentials. Consider moving these to environment variables for security.

### Output Files
Key output files that scripts depend on:
- `linkid_question_mapping.json` - Question text reference (301KB)
- `unmapped_uuid_fields.csv` - UUID fields needing mapping (11KB)
- `discovered_questionnaires.csv` - List of known questionnaires
- `seeds/questionnaire_metadata.csv` - Central metadata file

### Version Pattern
The `_v2` suffix indicates iterative improvement. When you see both v1 and v2, use v2.
