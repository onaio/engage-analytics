# Metrics Models

This document explains how the metrics layer works and how to run aggregate indicators.

## Overview

The metrics layer (`models/metrics/`) contains aggregate KPIs for reporting. These models build on top of the staging, intermediate, and marts layers to produce business metrics.

All 32 indicators from `engage-indicators.csv` are implemented (28 active, 4 skipped as not needed).

## Running Metrics

```bash
cd /Users/mberg/github/engage-analytics/dbt
source .envrc

# Run all metrics models
uv run dbt run --profiles-dir . --select "metrics.*"

# Run metrics + their upstream dependencies
uv run dbt run --profiles-dir . --select "+metrics.*"

# Run a specific metric
uv run dbt run --profiles-dir . --select active_providers

# Run using folder path
uv run dbt run --profiles-dir . --select "path:models/metrics"
```

## Indicator Coverage

| # | CSV Indicator | Status | Metric(s) |
|---|---------------|--------|-----------|
| 1 | Active users | ✅ | `active_providers` |
| 2 | Percent of active users | ✅ | `percent_active_providers` |
| 3 | Total Users | ✅ | `provider_count` |
| 4 | Clients registered | ✅ | `patient_count` |
| 5 | Encounters | ✅ | `client_encounters` |
| 6 | Number of IPC sessions | ✅ | `sessions_ipc` |
| 7 | Number of client eligible for IPC | ✅ | `clients_eligible_for_ipc` |
| 8 | Number of client eligible for SBIRT-MI | ✅ | `clients_sbirt_mi_eligible` |
| 9 | Number of client potentially eligible for SPI | ✅ | `clients_eligible_spi` |
| 10 | Number of client eligible for referral | ✅ | `clients_eligible_referral` |
| 11 | Number of client eligible for FWS | ✅ | `clients_eligible_fws` |
| 12 | Number of client offered IPC | ⛔ SKIP | Not needed |
| 13 | Number of client who accepted IPC | ✅ | `clients_accepted_ipc` |
| 14 | Number of client offered SBIRT-MI | ⛔ SKIP | Not needed |
| 15 | Number of client who accepted SBIRT-MI | ✅ | `clients_accepted_sbirt` |
| 16 | Number of client offered FWS | ⛔ SKIP | Not needed |
| 17 | Number of client who accepted FWS | ✅ | `clients_accepted_fws` |
| 18 | Number of client offered referral | ⛔ SKIP | Not needed |
| 19 | Number of client accepted referral | ✅ | `clients_accepted_referral` |
| 20 | Refusal rate | ✅ | `service_refusal_rate` |
| 21 | % access to care (Reach) | ✅ | `mwtool_*_conversion_rate` |
| 22 | Comorbidities | ✅ | `mwtool_comorbidity_rate` |
| 23 | Service delivery modality | ✅ | `encounters_in_person`, `encounters_video_telehealth`, `encounters_phone_telehealth` |
| 24 | Assessment outcome severe risk | ✅ | `phq9_clients_severe`, `gad7_clients_severe` |
| 25 | Assessment outcome moderate-high risk | ✅ | `phq9_clients_moderate_high`, `gad7_clients_moderate_high` |
| 26 | Assessment outcome low risk | ✅ | `phq9_clients_low`, `gad7_clients_low` |
| 27 | Assessment scores subsequent sessions | ✅ | `phq9_improvement_rate`, `phq9_remission_rate` |
| 28 | % finish all sessions IPC | ✅ | `ipc_completion_rate` |
| 29 | % follow-up | ✅ | `follow_up_completion_rate` |
| 30 | Providers using technology | ✅ | Same as #1 |
| 31 | Providers using technology across time | ✅ | Same as #2 |
| 32 | Screening location setting | ✅ | `screenings_total`, `screening_locations_count` |

## Available Models

### Eligibility Models
| Model | Description | Data Source |
|-------|-------------|-------------|
| `clients_eligible_for_ipc` | Clients with probable common mental health problems | mwTool (Questionnaire/1613532) |
| `clients_sbirt_mi_eligible` | Clients with alcohol or drug problems | mwTool |
| `clients_eligible_spi` | Clients with suicide risk | mwTool |
| `clients_eligible_referral` | Clients with severe mental health problems | mwTool |
| `clients_eligible_fws` | Clients with financial hardship | Financial Wellness Tool |

### Acceptance Models
| Model | Description | Data Source |
|-------|-------------|-------------|
| `clients_accepted_ipc` | Clients who accepted IPC | Planning Next Steps form |
| `clients_accepted_sbirt` | Clients who accepted SBIRT-MI | Planning Next Steps form |
| `clients_accepted_fws` | Clients who accepted Financial Wellness Services | Planning Next Steps form |
| `clients_accepted_referral` | Clients who accepted referral to MH specialist | Planning Next Steps form |

### Treatment & Progress Models
| Model | Description | Data Source |
|-------|-------------|-------------|
| `client_ipc_progress` | IPC session completion tracking (S1-S4) | IPC session questionnaires |
| `intervention_sessions` | All intervention sessions (IPC, SPI, SBIRT) | Various session questionnaires |
| `mwtool_to_intervention` | Links mwTool eligibility to treatment initiation | mwTool + session starts |
| `assessment_scores_all_sessions` | PHQ-9 scores across all sessions with improvement tracking | PHQ-9 questionnaires |
| `int_assessment_scores` | Assessment scores with severity classification | PHQ-9, GAD-7 questionnaires |

### Activity & Engagement Models
| Model | Description | Data Source |
|-------|-------------|-------------|
| `active_providers` | Provider activity status (30-day window) | Questionnaire responses |
| `encounters_by_delivery_format` | Encounters by modality (in-person, telehealth) | IPC Session 1 form |

### Follow-up & Outcomes Models
| Model | Description | Data Source |
|-------|-------------|-------------|
| `follow_up_1month` | 1-month follow-up task tracking | Tasks (code 040) |
| `mwtool_comorbidities` | Clients with multiple positive mwTool categories | mwTool |
| `service_refusal` | Service acceptance/refusal tracking | Planning Next Steps form |
| `screenings_by_location` | mwTool screenings by practitioner location | mwTool + Practitioners |

### Fact Tables
| Model | Description |
|-------|-------------|
| `fct_metrics_long` | All 64 metrics in long format for reporting |

## All Metrics in fct_metrics_long (64 total)

### System Use Metrics
| Metric | Unit | Description |
|--------|------|-------------|
| `provider_count` | count | Total registered providers |
| `patient_count` | count | Total registered clients |
| `active_providers` | count | Providers active in past 30 days |
| `percent_active_providers` | percent | % of providers active |
| `client_encounters` | count | Unique client encounters |

### Session Metrics
| Metric | Unit | Description |
|--------|------|-------------|
| `sessions_total` | count | Total counseling sessions |
| `sessions_ipc` | count | IPC sessions |
| `sessions_sbirt` | count | SBIRT sessions |
| `sessions_spi` | count | SPI sessions |
| `clients_receiving_counseling` | count | Unique clients in any session |

### Eligibility Metrics
| Metric | Unit | Description |
|--------|------|-------------|
| `clients_eligible_for_ipc` | count | Eligible for IPC (common MH) |
| `clients_sbirt_mi_eligible` | count | Eligible for SBIRT (alcohol/drug) |
| `clients_eligible_spi` | count | Eligible for SPI (suicide risk) |
| `clients_eligible_referral` | count | Eligible for referral (severe MH) |
| `clients_eligible_fws` | count | Eligible for FWS (financial hardship) |

### Service Acceptance Metrics
| Metric | Unit | Description |
|--------|------|-------------|
| `clients_accepted_ipc` | count | Accepted IPC |
| `clients_accepted_sbirt` | count | Accepted SBIRT-MI |
| `clients_accepted_fws` | count | Accepted Financial Wellness |
| `clients_accepted_referral` | count | Accepted referral to MH specialist |

### Service Refusal Metrics
| Metric | Unit | Description |
|--------|------|-------------|
| `service_refusal_ipc` | count | Declined IPC |
| `service_refusal_sbirt` | count | Declined SBIRT |
| `service_refusal_fws` | count | Declined FWS |
| `service_refusal_referral` | count | Declined referral |
| `service_refusal_rate` | percent | Overall refusal rate |

### Treatment Conversion Metrics
| Metric | Unit | Description |
|--------|------|-------------|
| `mwtool_ipc_eligible_initiated` | count | IPC-eligible who started treatment |
| `mwtool_ipc_conversion_rate` | percent | % IPC-eligible who started |
| `mwtool_sbirt_eligible_initiated` | count | SBIRT-eligible who started |
| `mwtool_sbirt_conversion_rate` | percent | % SBIRT-eligible who started |
| `mwtool_spi_eligible_initiated` | count | SPI-eligible who started |
| `mwtool_spi_conversion_rate` | percent | % SPI-eligible who started |

### IPC Completion Metrics
| Metric | Unit | Description |
|--------|------|-------------|
| `ipc_clients_started` | count | Clients who started IPC (session 1) |
| `ipc_clients_completed` | count | Clients who completed all 4 sessions |
| `ipc_completion_rate` | percent | % who completed all sessions |
| `ipc_retention_s1_to_s2` | percent | % who returned for session 2 |

### Assessment Risk Level Metrics
| Metric | Unit | Description |
|--------|------|-------------|
| `phq9_clients_severe` | count | PHQ-9 severe (20-27) |
| `phq9_clients_moderate_high` | count | PHQ-9 moderate/moderately severe (10-19) |
| `phq9_clients_low` | count | PHQ-9 mild/minimal (0-9) |
| `phq9_percent_severe` | percent | % PHQ-9 severe |
| `phq9_percent_moderate_high` | percent | % PHQ-9 moderate |
| `gad7_clients_severe` | count | GAD-7 severe (15-21) |
| `gad7_clients_moderate_high` | count | GAD-7 moderate (10-14) |
| `gad7_clients_low` | count | GAD-7 mild/minimal (0-9) |
| `gad7_percent_severe` | percent | % GAD-7 severe |
| `gad7_percent_moderate_high` | percent | % GAD-7 moderate |

### Treatment Outcome Metrics
| Metric | Unit | Description |
|--------|------|-------------|
| `phq9_clients_with_followup` | count | Clients with multiple PHQ-9 scores |
| `phq9_clients_improved` | count | Clients whose score improved |
| `phq9_improvement_rate` | percent | % who showed improvement |
| `phq9_remission_rate` | percent | % who achieved remission (score < 10) |
| `phq9_avg_score_improvement` | count | Average score improvement points |

### Comorbidity Metrics
| Metric | Unit | Description |
|--------|------|-------------|
| `mwtool_clients_with_comorbidity` | count | Clients with 2+ positive mwTool categories |
| `mwtool_comorbidity_rate` | percent | % with comorbidities |

### Follow-up Metrics
| Metric | Unit | Description |
|--------|------|-------------|
| `follow_up_tasks_total` | count | Total 1-month follow-up tasks |
| `follow_up_tasks_completed` | count | Completed follow-up tasks |
| `follow_up_completion_rate` | percent | % follow-up completed |

### Service Delivery Metrics
| Metric | Unit | Description |
|--------|------|-------------|
| `encounters_in_person` | count | In-person encounters |
| `encounters_video_telehealth` | count | Video telehealth encounters |
| `encounters_phone_telehealth` | count | Phone telehealth encounters |

### Screening Metrics
| Metric | Unit | Description |
|--------|------|-------------|
| `screenings_total` | count | Total mwTool screenings |
| `screening_locations_count` | count | Unique screening locations |

### Observation Metrics
| Metric | Unit | Description |
|--------|------|-------------|
| `observation_count` | count | Total computed observations |
| `observation_phq9_count` | count | PHQ-9 observations |
| `observation_gad7_count` | count | GAD-7 observations |
| `observation_mood_count` | count | Mood rating observations |
| `observation_ptsd_count` | count | PTSD (PCL-5) observations |

## Fact Table: fct_metrics_long

The `fct_metrics_long` model combines all metrics into a single long-format fact table for easy querying and reporting.

### Schema

| Column | Description |
|--------|-------------|
| `period_date` | Date of the metric |
| `organization_id` | Organization identifier |
| `metric_id` | Metric name (e.g., 'active_providers', 'patient_count') |
| `value` | Numeric value of the metric |
| `unit` | Unit type ('count', 'percent') |
| `method_version` | Version of the calculation method |
| `description` | Human-readable description |
| `status` | Status flag ('prod') |

### Querying

```sql
-- Get all metrics for an organization
select * from fct_metrics_long
where organization_id = 'org-123'
order by period_date desc, metric_id;

-- Get specific metric over time
select period_date, value
from fct_metrics_long
where metric_id = 'active_providers'
  and organization_id = 'org-123'
order by period_date;

-- Compare metrics across organizations
select organization_id, metric_id, value
from fct_metrics_long
where period_date = current_date
  and metric_id in ('active_providers', 'patient_count')
order by metric_id, organization_id;
```

## Metrics Catalog

Metrics are defined in `_metrics_catalog.yml` and processed by the `metrics_catalog()` macro. Each metric definition includes:

| Field | Description |
|-------|-------------|
| `id` | Unique metric identifier |
| `unit` | Unit type ('count', 'percent') |
| `grain` | Time granularity ('day') |
| `entity_keys` | Grouping dimensions (e.g., `[organization_id]`) |
| `source_model` | Model the metric is computed from |
| `expression` | SQL expression for count metrics |
| `numerator` / `denominator` | SQL expressions for percent metrics |
| `description` | Human-readable description |
| `version` | Method version for tracking changes |

### Example Definitions

```yaml
# Count metric
- id: provider_count
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: practitioners
  expression: "count(distinct id)"
  description: "Total number of unique providers"
  version: v1

# Percent metric
- id: percent_active_providers
  unit: percent
  grain: day
  entity_keys: [organization_id]
  source_model: active_providers
  numerator: "count(distinct case when is_active_30d then practitioner_id end)"
  denominator: "nullif(count(distinct practitioner_id), 0)"
  description: "Percentage of providers active in past 30 days"
  version: v1
```

## Adding a New Metric

### Option 1: Simple Metric (via catalog)

Add to `_metrics_catalog.yml`:

```yaml
- id: new_metric_name
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: existing_model
  expression: "count(distinct some_field)"
  description: "Description of what this metric measures"
  version: v1
```

Then rebuild:
```bash
uv run dbt run --profiles-dir . --select fct_metrics_long
```

### Option 2: Complex Metric (new model)

1. Create `models/metrics/new_metric.sql`:

```sql
{{
    config(
        materialized='view'
    )
}}

-- Description of what this metric measures

with source_data as (
  select
    some_field,
    some_date::date as period_date,
    organization_id
  from {{ ref('source_model') }}
  where some_condition
)

select
  period_date,
  organization_id,
  count(distinct some_field) as metric_value
from source_data
group by 1, 2
```

2. Add to catalog if it should appear in `fct_metrics_long`

3. Run:
```bash
uv run dbt run --profiles-dir . --select new_metric
```

## Dependencies

Metrics depend on upstream models:

```
staging (stg_*)
    │
    ▼
intermediate (int_*)
    │
    ▼
marts (patient, practitioners, encounters, qr_*)
    │
    ▼
metrics (active_providers, clients_*, fct_metrics_long)
```

To rebuild everything including dependencies:
```bash
uv run dbt run --profiles-dir . --select "+metrics.*"
```
