# Metrics Models

This document explains how the metrics layer works and how to run aggregate indicators.

## Overview

The metrics layer (`models/metrics/`) contains aggregate KPIs for reporting. These models build on top of the staging, intermediate, and marts layers to produce business metrics.

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

## Available Metrics

| Model | Description |
|-------|-------------|
| `active_providers` | Providers who submitted questionnaire responses in past 30 days |
| `clients_eligible_for_ipc` | Clients eligible for Interpersonal Counseling based on Mental Wellness Tool |
| `clients_accepted_ipc` | Clients who accepted IPC via Planning Next Steps questionnaire |
| `clients_eligible_fws` | Clients eligible for Financial Wellness Services |
| `clients_accepted_fws` | Clients who accepted Financial Wellness Services |
| `clients_eligible_referral` | Clients eligible for referral |
| `clients_accepted_referral` | Clients who accepted referral |
| `clients_eligible_spi` | Clients eligible for SPI |
| `clients_sbirt_mi_eligible` | Clients eligible for SBIRT/MI intervention |
| `encounters_by_delivery_format` | Encounters broken down by delivery method |
| `fct_metrics_long` | All metrics combined into one fact table |
| `fct_metrics_long_incremental` | Incremental version of the metrics fact table |

## Metric Types

### Activity Metrics

**`active_providers`** - Tracks provider engagement
- Counts providers who submitted questionnaire responses in the past 30 days
- Uses a rolling 30-day window from current date
- Grouped by organization

```sql
-- Key logic
select
  p.id as practitioner_id,
  p.organization_id,
  case
    when qa.practitioner_id is not null then true
    else false
  end as is_active_30d
from practitioners p
left join qr_activity qa on qa.practitioner_id = p.id
```

### Eligibility Metrics

**`clients_eligible_for_ipc`** - IPC eligibility based on Mental Wellness Tool
- Source: Questionnaire/1613532 (Mental Wellness Tool)
- Eligibility criteria: `no_mental_problem = false`
- Tracks cumulative patients eligible as of each date

**`clients_eligible_fws`** - Financial Wellness eligibility
- Based on Financial Wellness Tool questionnaire responses

**`clients_eligible_referral`** - Referral eligibility

**`clients_sbirt_mi_eligible`** - SBIRT/MI intervention eligibility

### Acceptance Metrics

**`clients_accepted_ipc`** - IPC acceptance
- Source: Planning Next Steps questionnaire
- Criteria: `planning_next_steps...did_the_client_ = 'true'`

**`clients_accepted_fws`** - Financial Wellness acceptance

**`clients_accepted_referral`** - Referral acceptance

### Encounter Metrics

**`encounters_by_delivery_format`** - Breaks down encounters by delivery method (in-person, telehealth, etc.)

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
