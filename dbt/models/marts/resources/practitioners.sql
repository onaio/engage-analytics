{{ config(materialized='incremental', unique_key='id') }}

with
codes as (
  {{ resource_level_2_extraction(
        'stg_practitioner',
        ['name','telecom']
  ) }}
)

select
  p.id,
  p.active,
  oa.organization_id,
  oa.location_id,
  pc.name_family,
  trim(pc.name_given, '[""]') as name_given,
  pc.telecom_value as phone_number,
  greatest(
    p._airbyte_emitted_at,
    coalesce(oa._airbyte_emitted_at, '1900-01-01'),
    coalesce(pr._airbyte_emitted_at, '1900-01-01')
  ) as _airbyte_emitted_at,
  pr.coding_code    as role_id,
  pr.coding_display as role
from {{ ref('stg_practitioner') }} p
left join {{ ref('current_practitioner_role') }} pr
  on pr.practitioner_id = p.id
left join {{ ref('organization_affiliations') }} oa
  on pr.organization_id = oa.organization_id
left join codes as pc
  on pc.resource_id = p.id
{% if is_incremental() %}
where p._airbyte_emitted_at > (
  select coalesce(max(_airbyte_emitted_at),'1900-01-01') from {{ this }}
)
{% endif %}
