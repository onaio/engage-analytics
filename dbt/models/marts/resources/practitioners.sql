-- ABOUTME: Extracts practitioner data with organization assignment
-- ABOUTME: Organization comes from CareTeam membership via int_practitioner_organization

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
  -- Organization from CareTeam membership (more complete than PractitionerRole)
  coalesce(po.organization_id, oa.organization_id) as organization_id,
  oa.location_id,
  pc.name_family,
  trim(pc.name_given, '[""]') as name_given,
  pc.telecom_value as phone_number,
  greatest(
    p._airbyte_emitted_at,
    coalesce(po._airbyte_emitted_at, '1900-01-01'),
    coalesce(oa._airbyte_emitted_at, '1900-01-01'),
    coalesce(pr._airbyte_emitted_at, '1900-01-01')
  ) as _airbyte_emitted_at,
  pr.coding_code    as role_id,
  pr.coding_display as role
from {{ ref('stg_practitioner') }} p
-- CareTeam-based organization mapping (primary source)
left join {{ ref('int_practitioner_organization') }} po
  on po.practitioner_id = p.id
-- PractitionerRole for role info and fallback org
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
