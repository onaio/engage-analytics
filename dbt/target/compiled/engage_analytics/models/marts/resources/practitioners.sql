

with
codes as (
  select
    id::text as resource_id,
        name::jsonb ->> 'family' as name_family,
        name::jsonb ->> 'use' as name_use,
        name::jsonb ->> 'given' as name_given,
        telecom::jsonb ->> 'value' as telecom_value,
        telecom::jsonb ->> 'system' as telecom_system,
    /* use the column your staging models actually expose */
    _airbyte_emitted_at
  from "airbyte"."engage_analytics_engage_analytics_stg"."stg_practitioner"
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
from "airbyte"."engage_analytics_engage_analytics_stg"."stg_practitioner" p
left join "airbyte"."engage_analytics_engage_analytics_mart"."current_practitioner_role" pr
  on pr.practitioner_id = p.id
left join "airbyte"."engage_analytics_engage_analytics_mart"."organization_affiliations" oa
  on pr.organization_id = oa.organization_id
left join codes as pc
  on pc.resource_id = p.id

where p._airbyte_emitted_at > (
  select coalesce(max(_airbyte_emitted_at),'1900-01-01') from "airbyte"."engage_analytics_engage_analytics_mart"."practitioners"
)
