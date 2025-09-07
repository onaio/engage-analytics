

select
  s.id,
  
  split_part(s.organization::jsonb->>'reference', '/', 2)
                 as organization_id,
  
  split_part((s.location::jsonb -> 0)::jsonb->>'reference', '/', 2)
       as location_id,   -- first array entry
  s._airbyte_emitted_at
from "airbyte"."engage_analytics_engage_analytics_stg"."stg_organization_affiliations" s

where s._airbyte_emitted_at > (
  select coalesce(max(_airbyte_emitted_at),'1900-01-01') from "airbyte"."engage_analytics_engage_analytics_mart"."organization_affiliations"
)
