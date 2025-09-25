-- models/metrics/fct_metrics_long_incremental.sql
-- Reprocess last 7 days for late-arriving data

with

base_practitioners as (
  select
    current_date as period_date,organization_id,
    count(distinct id) as provider_count_value
  from "airbyte"."engage_analytics_engage_analytics_mart"."practitioners"
  where 1=1
    
    and active = 'true'
    
    
    
      -- For dimension tables, always use current snapshot
      and 1=1
    
    
  group by organization_id
),
base_patient as (
  select
    current_date as period_date,practitioner_organization_id,
    count(distinct id) as patient_count_value
  from "airbyte"."engage_analytics_engage_analytics_mart"."patient"
  where 1=1
    
    and active = 'true'
    
    
    
      -- For dimension tables, always use current snapshot
      and 1=1
    
    
  group by practitioner_organization_id
),
base_encounters as (
  select
    period_start::date as period_date,practitioner_organization_id,
    count(distinct subject_id) as client_encounters_value
  from "airbyte"."engage_analytics_engage_analytics_mart"."encounters"
  where 1=1
    
    and period_start is not null
    
    
    
      -- For encounters, reprocess recent days for late-arriving data
      and period_start::date >= current_date - interval '7 days'
    
    
  group by
    period_start::date, practitioner_organization_id
),
base_active_providers as (
  select
    current_date as period_date,organization_id,
    count(distinct case when is_active_30d then practitioner_id end) as active_providers_value,
    (count(distinct case when is_active_30d then practitioner_id end)::decimal / nullif(count(distinct practitioner_id), 0)) * 100 as percent_active_providers_value
  from "airbyte"."engage_analytics"."active_providers"
  where 1=1
    
    
    
      -- For dimension tables, always use current snapshot
      and 1=1
    
    
  group by organization_id
),
base_clients_eligible_for_ipc as (
  select
    current_date as period_date,organization_id,
    count(distinct subject_patient_id) as clients_eligible_for_ipc_value
  from "airbyte"."engage_analytics"."clients_eligible_for_ipc"
  where 1=1
    
    
    
      -- For dimension tables, always use current snapshot
      and 1=1
    
    
  group by organization_id
),
base_clients_sbirt_mi_eligible as (
  select
    current_date as period_date,organization_id,
    count(distinct subject_patient_id) as clients_sbirt_mi_eligible_value
  from "airbyte"."engage_analytics"."clients_sbirt_mi_eligible"
  where 1=1
    
    
    
      -- For dimension tables, always use current snapshot
      and 1=1
    
    
  group by organization_id
),
base_clients_eligible_spi as (
  select
    current_date as period_date,organization_id,
    count(distinct subject_patient_id) as clients_eligible_spi_value
  from "airbyte"."engage_analytics"."clients_eligible_spi"
  where 1=1
    
    
    
      -- For dimension tables, always use current snapshot
      and 1=1
    
    
  group by organization_id
),
base_clients_eligible_referral as (
  select
    current_date as period_date,organization_id,
    count(distinct subject_patient_id) as clients_eligible_referral_value
  from "airbyte"."engage_analytics"."clients_eligible_referral"
  where 1=1
    
    
    
      -- For dimension tables, always use current snapshot
      and 1=1
    
    
  group by organization_id
),
base_clients_eligible_fws as (
  select
    current_date as period_date,organization_id,
    count(distinct subject_patient_id) as clients_eligible_fws_value
  from "airbyte"."engage_analytics"."clients_eligible_fws"
  where 1=1
    
    
    
      -- For dimension tables, always use current snapshot
      and 1=1
    
    
  group by organization_id
),
base_encounters_by_delivery_format as (
  select
    current_date as period_date,organization_id,
    count(distinct case when format_you_deliver = 'In-person' then subject_patient_id end) as encounters_in_person_value,
    count(distinct case when format_you_deliver = 'Video telehealth' then subject_patient_id end) as encounters_video_telehealth_value,
    count(distinct case when format_you_deliver = 'Phone telehealth' then subject_patient_id end) as encounters_phone_telehealth_value
  from "airbyte"."engage_analytics"."encounters_by_delivery_format"
  where 1=1
    
    
    
      -- For dimension tables, always use current snapshot
      and 1=1
    
    
  group by organization_id
)
  select
    period_date,
    organization_id,
    'provider_count' as metric_id,
    provider_count_value::numeric as value,
    'count' as unit,
    'v1' as method_version,
    'Total number of unique providers' as description,
    'prod' as status,
    current_timestamp as _last_updated
  from base_practitioners
  
  
    -- Only include dates we're updating
    where 1=1
    and period_date = current_date
  
  
  union all
  select
    period_date,
    practitioner_organization_id,
    'patient_count' as metric_id,
    patient_count_value::numeric as value,
    'count' as unit,
    'v1' as method_version,
    'Total number of unique patients' as description,
    'prod' as status,
    current_timestamp as _last_updated
  from base_patient
  
  
    -- Only include dates we're updating
    where 1=1
    and period_date = current_date
  
  
  union all
  select
    period_date,
    practitioner_organization_id,
    'client_encounters' as metric_id,
    client_encounters_value::numeric as value,
    'count' as unit,
    'v1' as method_version,
    'Total unique clients seen (one encounter per patient per day)' as description,
    'prod' as status,
    current_timestamp as _last_updated
  from base_encounters
  
  
    -- Only include dates we're updating
    where 1=1
    and period_date >= current_date - interval '7 days'
  
  
  union all
  select
    period_date,
    organization_id,
    'active_providers' as metric_id,
    active_providers_value::numeric as value,
    'count' as unit,
    'v1' as method_version,
    'Providers who submitted QRs in past 30 days' as description,
    'prod' as status,
    current_timestamp as _last_updated
  from base_active_providers
  
  
    -- Only include dates we're updating
    where 1=1
    and period_date = current_date
  
  
  union all
  select
    period_date,
    organization_id,
    'percent_active_providers' as metric_id,
    percent_active_providers_value::numeric as value,
    'percent' as unit,
    'v1' as method_version,
    'Percentage of providers active in past 30 days' as description,
    'prod' as status,
    current_timestamp as _last_updated
  from base_active_providers
  
  
    -- Only include dates we're updating
    where 1=1
    and period_date = current_date
  
  
  union all
  select
    period_date,
    organization_id,
    'clients_eligible_for_ipc' as metric_id,
    clients_eligible_for_ipc_value::numeric as value,
    'count' as unit,
    'v1' as method_version,
    'Clients eligible for IPC (Integrated Primary Care)' as description,
    'prod' as status,
    current_timestamp as _last_updated
  from base_clients_eligible_for_ipc
  
  
    -- Only include dates we're updating
    where 1=1
    and period_date = current_date
  
  
  union all
  select
    period_date,
    organization_id,
    'clients_sbirt_mi_eligible' as metric_id,
    clients_sbirt_mi_eligible_value::numeric as value,
    'count' as unit,
    'v1' as method_version,
    'Clients eligible for SBIRT/MI (alcohol or drug problems)' as description,
    'prod' as status,
    current_timestamp as _last_updated
  from base_clients_sbirt_mi_eligible
  
  
    -- Only include dates we're updating
    where 1=1
    and period_date = current_date
  
  
  union all
  select
    period_date,
    organization_id,
    'clients_eligible_spi' as metric_id,
    clients_eligible_spi_value::numeric as value,
    'count' as unit,
    'v1' as method_version,
    'Clients eligible for SPI (Suicide Prevention Intervention)' as description,
    'prod' as status,
    current_timestamp as _last_updated
  from base_clients_eligible_spi
  
  
    -- Only include dates we're updating
    where 1=1
    and period_date = current_date
  
  
  union all
  select
    period_date,
    organization_id,
    'clients_eligible_referral' as metric_id,
    clients_eligible_referral_value::numeric as value,
    'count' as unit,
    'v1' as method_version,
    'Clients eligible for referral (severe mental health)' as description,
    'prod' as status,
    current_timestamp as _last_updated
  from base_clients_eligible_referral
  
  
    -- Only include dates we're updating
    where 1=1
    and period_date = current_date
  
  
  union all
  select
    period_date,
    organization_id,
    'clients_eligible_fws' as metric_id,
    clients_eligible_fws_value::numeric as value,
    'count' as unit,
    'v1' as method_version,
    'Clients eligible for FWS (Financial Wellness Services)' as description,
    'prod' as status,
    current_timestamp as _last_updated
  from base_clients_eligible_fws
  
  
    -- Only include dates we're updating
    where 1=1
    and period_date = current_date
  
  
  union all
  select
    period_date,
    organization_id,
    'encounters_in_person' as metric_id,
    encounters_in_person_value::numeric as value,
    'count' as unit,
    'v1' as method_version,
    'Daily in-person encounters' as description,
    'prod' as status,
    current_timestamp as _last_updated
  from base_encounters_by_delivery_format
  
  
    -- Only include dates we're updating
    where 1=1
    and period_date = current_date
  
  
  union all
  select
    period_date,
    organization_id,
    'encounters_video_telehealth' as metric_id,
    encounters_video_telehealth_value::numeric as value,
    'count' as unit,
    'v1' as method_version,
    'Daily video telehealth encounters' as description,
    'prod' as status,
    current_timestamp as _last_updated
  from base_encounters_by_delivery_format
  
  
    -- Only include dates we're updating
    where 1=1
    and period_date = current_date
  
  
  union all
  select
    period_date,
    organization_id,
    'encounters_phone_telehealth' as metric_id,
    encounters_phone_telehealth_value::numeric as value,
    'count' as unit,
    'v1' as method_version,
    'Daily phone telehealth encounters' as description,
    'prod' as status,
    current_timestamp as _last_updated
  from base_encounters_by_delivery_format
  
  
    -- Only include dates we're updating
    where 1=1
    and period_date = current_date
  
  
  