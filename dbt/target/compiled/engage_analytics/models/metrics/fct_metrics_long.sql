-- models/metrics/fct_metrics_long.sql
with

base_practitioners as (
  select
    current_date as period_date,
    organization_id,
    count(distinct id) as provider_count_value
  from "airbyte"."engage_analytics_engage_analytics_mart"."practitioners"
  
  where active = 'true'
  
  group by organization_id
),
base_patient as (
  select
    current_date as period_date,
    practitioner_organization_id,
    count(distinct id) as patient_count_value
  from "airbyte"."engage_analytics_engage_analytics_mart"."patient"
  
  where active = 'true'
  
  group by practitioner_organization_id
)
  select
    period_date,
    organization_id,
    'provider_count' as metric_id,
    provider_count_value::numeric as value,
    'count' as unit,
    'v1' as method_version,
    'Total number of unique providers' as description,
    'prod' as status
  from base_practitioners
  union all
  select
    period_date,
    practitioner_organization_id,
    'patient_count' as metric_id,
    patient_count_value::numeric as value,
    'count' as unit,
    'v1' as method_version,
    'Total number of unique patients' as description,
    'prod' as status
  from base_patient
  