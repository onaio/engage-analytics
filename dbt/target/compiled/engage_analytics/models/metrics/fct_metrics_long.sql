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
),
base_encounters as (
  select
    period_start::date as period_date,
    practitioner_organization_id,
    count(distinct subject_id) as client_encounters_value
  from "airbyte"."engage_analytics_engage_analytics_mart"."encounters"
  
  where period_start is not null
  
  group by
    period_start::date, practitioner_organization_id
),
base_active_providers as (
  select
    current_date as period_date,
    organization_id,
    count(distinct case when is_active_30d then practitioner_id end) as active_providers_value,
    count(distinct case when is_active_30d then practitioner_id end) as percent_active_providers_numerator,
    nullif(count(distinct practitioner_id), 0) as percent_active_providers_denominator
  from "airbyte"."engage_analytics"."active_providers"
  
  group by organization_id
),
base_clients_with_mental_health as (
  select
    period_date,
    organization_id,
    count(distinct subject_patient_id) as clients_with_probable_mental_health_value
  from "airbyte"."engage_analytics"."clients_with_mental_health"
  
  group by
    period_date, organization_id
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
  union all
  select
    period_date,
    practitioner_organization_id,
    'client_encounters' as metric_id,
    client_encounters_value::numeric as value,
    'count' as unit,
    'v1' as method_version,
    'Total unique clients seen (one encounter per patient per day)' as description,
    'prod' as status
  from base_encounters
  union all
  select
    period_date,
    organization_id,
    'active_providers' as metric_id,
    active_providers_value::numeric as value,
    'count' as unit,
    'v1' as method_version,
    'Providers who submitted QRs in past 30 days' as description,
    'prod' as status
  from base_active_providers
  union all
  select
    period_date,
    organization_id,
    'percent_active_providers' as metric_id,
    (percent_active_providers_numerator::numeric / percent_active_providers_denominator * 100) as value,
    'percent' as unit,
    'v1' as method_version,
    'Percentage of providers active in past 30 days' as description,
    'prod' as status
  from base_active_providers
  union all
  select
    period_date,
    organization_id,
    'clients_with_probable_mental_health' as metric_id,
    clients_with_probable_mental_health_value::numeric as value,
    'count' as unit,
    'v1' as method_version,
    'Clients with probable mental health issues (MW Tool)' as description,
    'prod' as status
  from base_clients_with_mental_health
  