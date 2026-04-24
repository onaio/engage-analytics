-- ABOUTME: Extracts practitioner data with organization assignment
-- ABOUTME: Only includes Engage practitioners (those with CareTeam membership)
-- ABOUTME: Organization comes from CareTeam membership via int_practitioner_organization

{{ config(materialized='incremental', unique_key='id') }}

with
names as (
  select
    id as resource_id,
    name::jsonb ->> 'family' as name_family,
    trim(name::jsonb ->> 'given', '[""]') as name_given,
    _airbyte_emitted_at
  from {{ ref('stg_practitioner') }}
  where jsonb_typeof(name::jsonb) = 'object'
),

phones as (
  select
    id as resource_id,
    telecom::jsonb ->> 'value' as phone_number
  from {{ ref('stg_practitioner') }}
  where jsonb_typeof(telecom::jsonb) = 'object'
    and telecom::jsonb ->> 'system' = 'phone'
),

emails as (
  select
    id as resource_id,
    telecom::jsonb ->> 'value' as email
  from {{ ref('stg_practitioner') }}
  where jsonb_typeof(telecom::jsonb) = 'object'
    and telecom::jsonb ->> 'system' = 'email'
)

select
  p.id,
  p.active,
  -- Organization from CareTeam membership (more complete than PractitionerRole)
  coalesce(po.organization_id, oa.organization_id) as organization_id,
  oa.location_id,
  n.name_family,
  n.name_given,
  ph.phone_number,
  em.email,
  greatest(
    p._airbyte_emitted_at,
    coalesce(po._airbyte_emitted_at, '1900-01-01'),
    coalesce(oa._airbyte_emitted_at, '1900-01-01'),
    coalesce(pr._airbyte_emitted_at, '1900-01-01')
  ) as _airbyte_emitted_at,
  pr.coding_code    as role_id,
  pr.coding_display as role
from {{ ref('stg_practitioner') }} p
-- Only include practitioners with CareTeam membership (Engage practitioners)
inner join {{ ref('int_practitioner_organization') }} po
  on po.practitioner_id = p.id
-- PractitionerRole for role info and fallback org
left join {{ ref('current_practitioner_role') }} pr
  on pr.practitioner_id = p.id
left join {{ ref('organization_affiliations') }} oa
  on pr.organization_id = oa.organization_id
left join names n
  on n.resource_id = p.id
left join phones ph
  on ph.resource_id = p.id
left join emails em
  on em.resource_id = p.id
{% if is_incremental() %}
where p._airbyte_emitted_at > (
  select coalesce(max(_airbyte_emitted_at),'1900-01-01') from {{ this }}
)
{% endif %}
