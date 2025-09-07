




select
    resource::jsonb ->> 'id' as id,
    resource::jsonb ->> 'meta' as meta,
    resource::jsonb ->> 'active' as active,
    resource::jsonb ->> 'identifier' as identifier,
    resource::jsonb ->> 'resourceType' as resourceType,
    jsonb_array_elements(resource::jsonb -> 'name')::jsonb as name,
    jsonb_array_elements(resource::jsonb -> 'telecom')::jsonb as telecom,
_airbyte_extracted_at as _airbyte_emitted_at
from "airbyte"."airbyte"."practitioner"


  where _airbyte_extracted_at > (select max(_airbyte_emitted_at) from "airbyte"."engage_analytics_engage_analytics_stg"."stg_practitioner")
