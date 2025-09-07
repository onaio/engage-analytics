

select
  spr.id,
  spr.active,
  spr.resourceType as resourcetype,
  -- first coding entry
  (spr.code::jsonb -> 'coding' -> 0 ->> 'code')    as coding_code,
  (spr.code::jsonb -> 'coding' -> 0 ->> 'display') as coding_display,
  
  split_part(spr.organization::jsonb->>'reference', '/', 2)
  as organization_id,
  
  split_part(spr.practitioner::jsonb->>'reference', '/', 2)
  as practitioner_id,
  (spr.meta::jsonb ->> 'lastUpdated') as meta_lastupdated,
  -- include the next line only if you actually want this column:
  -- (spr.meta::jsonb ->> 'source')       as meta_source,
  spr._airbyte_emitted_at
from "airbyte"."engage_analytics_engage_analytics_stg"."stg_practitioner_roles" spr

where spr._airbyte_emitted_at > (
  select coalesce(max(_airbyte_emitted_at), '1900-01-01')
  from "airbyte"."engage_analytics_engage_analytics_mart"."practitioner_role"
)
