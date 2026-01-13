# Engage Analytics dbt Project

## Overview
This is a dbt (data build tool) project for Engage Analytics that transforms healthcare data from various sources into analytical models. The project follows the standard dbt structure with staging, intermediate, and marts layers.

## Project Structure

```
dbt/
├── models/
│   ├── staging/        # Raw data extraction and initial transformations
│   ├── intermediate/    # Business logic and data cleaning
│   ├── marts/          # Final analytical models for consumption
│   │   ├── qr_named/   # Questionnaire response models with readable names
│   │   └── resources/  # FHIR resource models (patient, encounter, etc.)
│   └── sources/        # Source definitions
├── data/              # Seed data files
├── macros/            # Reusable SQL macros and functions
└── profiles.yml       # Database connection configuration
```

## Key Models

### Patient Resource (`models/marts/resources/patient.sql`)
- Incremental model that tracks patient demographic and administrative data
- Key fields: id, gender, birth_date, name, phone_number, location_id
- Joins with encounter tags for practitioner and organization associations

### Questionnaire Responses
- Multiple models in `marts/qr_named/` for different questionnaires
- Registration info captures PII data including names, DOB, addresses, Medicaid numbers
- Uses macros to pivot questionnaire responses into wide format with readable column names

## Database Configuration
- Database: airbyte
- Schema: engage_analytics_engage_analytics_mart (for marts)
- Requires environment variables: DBT_HOST, DBT_DATABASE
- Uses PostgreSQL with password authentication

## Common Commands
```bash
# Run all models
cd dbt && DBT_DATABASE=airbyte uv run dbt run --profiles-dir .

# Run specific model
cd dbt && DBT_DATABASE=airbyte uv run dbt run --profiles-dir . --select model_name

# Run seed data
cd dbt && DBT_DATABASE=airbyte uv run dbt seed --profiles-dir .

# Compile models
cd dbt && DBT_DATABASE=airbyte uv run dbt compile --profiles-dir .
```

## Data Privacy Considerations

### PII Fields Requiring Masking
The following fields contain personally identifiable information:
- **Patient Resource**: name_family, name_given, phone_number, birth_date
- **Registration Form**: 
  - Unique ID
  - First name, Middle name, Last name
  - Date of birth
  - Phone number
  - Email address
  - Physical address
  - Zip code
  - Medicaid number

### Anonymization Strategy
- Create `patient_anon` model that masks PII fields
- Replace birth_date with calculated age in years
- Hash or remove direct identifiers
- Maintain referential integrity through surrogate keys

## Macros
- `resource_level_1_extraction`: Extracts top-level JSONB fields from FHIR resources
- `meta_tag_pivot`: Pivots encounter tags into columns
- `build_qr_wide_readable`: Creates wide-format questionnaire response views with readable column names

## Dependencies
- dbt-core
- dbt-postgres
- dbt-utils package
- Python environment managed with uv
- direnv for environment variable management