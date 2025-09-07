




with previous_extraction as (
  select
    resource::jsonb ->> 'id' as id,
    resource::jsonb ->> 'meta' as meta,
    resource::jsonb ->> 'description' as description,
    resource::jsonb ->> 'partOf' as partOf,
    resource::jsonb ->> 'resourceType' as resourceType,
    resource::jsonb ->> 'status' as status,
    resource::jsonb ->> 'physicalType' as physicalType,
    resource::jsonb ->> 'name' as name,
    jsonb_array_elements(resource::jsonb -> 'alias')::jsonb as alias,
    jsonb_array_elements(resource::jsonb -> 'identifier')::jsonb as identifier,
  _airbyte_extracted_at as _airbyte_emitted_at
  from "airbyte"."airbyte"."location"
)
select
  *,
  partof::jsonb -> 'display'   as parent_name,
  
  split_part(partof::jsonb->>'reference', '/', 2)
 as parent_id,
  partof::jsonb -> 'reference' as parent_reference
from previous_extraction

where _airbyte_emitted_at > (select coalesce(max(_airbyte_emitted_at),'1900-01-01') from "airbyte"."engage_analytics_engage_analytics_stg"."stg_locations")
