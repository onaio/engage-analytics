{{ config(materialized='incremental', unique_key='id') }}

with
tags as (
  {{ meta_tag_pivot('stg_encounter') }}
),
codes as (
  -- Only json OBJECT columns here (arrays like reasonCode/participant are handled in SELECT)
  {{ resource_level_2_extraction(
        'stg_encounter',
        ['class','priority','period','serviceprovider']
  ) }}
)

select
  e.id,
  e.status,
  -- reasonCode is typically an array; grab the first element’s 'text' safely
  (e.reasoncode::jsonb -> 0 ->> 'text') as reasoncode,
  {{ find_resource_name('e.subject') }} as subject_type,
  {{ find_resource_id('e.subject') }}   as subject_id,
  date_trunc('month', (c.period_start)::date) as month,
  -- serviceType coding (jsonb)
  (e.servicetype::jsonb -> 'coding' -> 0 ->> 'code')    as service_coding,
  (e.servicetype::jsonb -> 'coding' -> 0 ->> 'display') as service_display,

  -- provider / class / priority decoded in 'codes' CTE
  c.serviceprovider_reference,
  c.class_code,
  c.priority_text,
  c.period_start,
  c.period_end,

  -- tags (meta->tag)
  t.practitioner_location_id as location_id,
  t.practitioner_organization_id,
  t.practitioner_careteam_id,
  t.practitioner_id,

  -- keep the raw participant json for downstream use (it’s an array)
  e.participant,
  e._airbyte_emitted_at
from {{ ref('stg_encounter') }} e
left join tags  t on t.resource_id = e.id
left join codes c on c.resource_id = e.id
{% if is_incremental() %}
where e._airbyte_emitted_at > (
  select coalesce(max(_airbyte_emitted_at), '1900-01-01') from {{ this }}
)
{% endif %}
