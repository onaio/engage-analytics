

with
codes as (
  select
    id::text as resource_id,
        type::jsonb ->> 'coding' as type_coding,
    /* use the column your staging models actually expose */
    _airbyte_emitted_at
  from "airbyte"."engage_analytics_engage_analytics_stg"."stg_organizations"
)

select
  o.id,
  o.name,
  o.active,
  c.*
from "airbyte"."engage_analytics_engage_analytics_stg"."stg_organizations" as o
left join codes c on c.resource_id = o.id 

where o._airbyte_emitted_at > (
  select coalesce(max(_airbyte_emitted_at),'1900-01-01') from "airbyte"."engage_analytics_engage_analytics_mart"."organizations"
)
