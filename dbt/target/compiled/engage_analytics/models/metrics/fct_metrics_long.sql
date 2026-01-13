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
base_clients_eligible_for_ipc as (
  select
    period_date,
    organization_id,
    count(distinct subject_patient_id) as clients_eligible_for_ipc_value
  from "airbyte"."engage_analytics"."clients_eligible_for_ipc"
  
  group by
    period_date, organization_id
),
base_clients_accepted_ipc as (
  select
    current_date as period_date,
    organization_id,
    count(distinct subject_patient_id) as clients_accepted_ipc_value
  from "airbyte"."engage_analytics"."clients_accepted_ipc"
  
  group by organization_id
),
base_clients_sbirt_mi_eligible as (
  select
    period_date,
    organization_id,
    count(distinct subject_patient_id) as clients_sbirt_mi_eligible_value
  from "airbyte"."engage_analytics"."clients_sbirt_mi_eligible"
  
  group by
    period_date, organization_id
),
base_clients_eligible_spi as (
  select
    period_date,
    organization_id,
    count(distinct subject_patient_id) as clients_eligible_spi_value
  from "airbyte"."engage_analytics"."clients_eligible_spi"
  
  group by
    period_date, organization_id
),
base_clients_eligible_referral as (
  select
    period_date,
    organization_id,
    count(distinct subject_patient_id) as clients_eligible_referral_value
  from "airbyte"."engage_analytics"."clients_eligible_referral"
  
  group by
    period_date, organization_id
),
base_clients_eligible_fws as (
  select
    period_date,
    organization_id,
    count(distinct subject_patient_id) as clients_eligible_fws_value
  from "airbyte"."engage_analytics"."clients_eligible_fws"
  
  group by
    period_date, organization_id
),
base_encounters_by_delivery_format as (
  select
    session_date as period_date,
    organization_id,
    count(distinct case when format_you_deliver = 'In-person' then subject_patient_id end) as encounters_in_person_value,
    count(distinct case when format_you_deliver = 'Video telehealth' then subject_patient_id end) as encounters_video_telehealth_value,
    count(distinct case when format_you_deliver = 'Phone telehealth' then subject_patient_id end) as encounters_phone_telehealth_value
  from "airbyte"."engage_analytics"."encounters_by_delivery_format"
  
  group by
    session_date, organization_id
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
    'clients_eligible_for_ipc' as metric_id,
    clients_eligible_for_ipc_value::numeric as value,
    'count' as unit,
    'v1' as method_version,
    'Clients eligible for IPC (Integrated Primary Care)' as description,
    'prod' as status
  from base_clients_eligible_for_ipc
  union all
  select
    period_date,
    organization_id,
    'clients_accepted_ipc' as metric_id,
    clients_accepted_ipc_value::numeric as value,
    'count' as unit,
    'v1' as method_version,
    'Clients who accepted IPC (scheduled IPC session)' as description,
    'prod' as status
  from base_clients_accepted_ipc
  union all
  select
    period_date,
    organization_id,
    'clients_sbirt_mi_eligible' as metric_id,
    clients_sbirt_mi_eligible_value::numeric as value,
    'count' as unit,
    'v1' as method_version,
    'Clients eligible for SBIRT/MI (alcohol or drug problems)' as description,
    'prod' as status
  from base_clients_sbirt_mi_eligible
  union all
  select
    period_date,
    organization_id,
    'clients_eligible_spi' as metric_id,
    clients_eligible_spi_value::numeric as value,
    'count' as unit,
    'v1' as method_version,
    'Clients eligible for SPI (Suicide Prevention Intervention)' as description,
    'prod' as status
  from base_clients_eligible_spi
  union all
  select
    period_date,
    organization_id,
    'clients_eligible_referral' as metric_id,
    clients_eligible_referral_value::numeric as value,
    'count' as unit,
    'v1' as method_version,
    'Clients eligible for referral (severe mental health)' as description,
    'prod' as status
  from base_clients_eligible_referral
  union all
  select
    period_date,
    organization_id,
    'clients_eligible_fws' as metric_id,
    clients_eligible_fws_value::numeric as value,
    'count' as unit,
    'v1' as method_version,
    'Clients eligible for FWS (Financial Wellness Services)' as description,
    'prod' as status
  from base_clients_eligible_fws
  union all
  select
    period_date,
    organization_id,
    'encounters_in_person' as metric_id,
    encounters_in_person_value::numeric as value,
    'count' as unit,
    'v1' as method_version,
    'Daily in-person encounters' as description,
    'prod' as status
  from base_encounters_by_delivery_format
  union all
  select
    period_date,
    organization_id,
    'encounters_video_telehealth' as metric_id,
    encounters_video_telehealth_value::numeric as value,
    'count' as unit,
    'v1' as method_version,
    'Daily video telehealth encounters' as description,
    'prod' as status
  from base_encounters_by_delivery_format
  union all
  select
    period_date,
    organization_id,
    'encounters_phone_telehealth' as metric_id,
    encounters_phone_telehealth_value::numeric as value,
    'count' as unit,
    'v1' as method_version,
    'Daily phone telehealth encounters' as description,
    'prod' as status
  from base_encounters_by_delivery_format
  