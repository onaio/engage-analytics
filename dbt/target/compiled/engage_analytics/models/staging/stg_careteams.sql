




select
    resource::jsonb ->> 'id' as id,
    resource::jsonb ->> 'meta' as meta,
    resource::jsonb ->> 'name' as name,
    resource::jsonb ->> 'status' as status,
    resource::jsonb ->> 'identifier' as identifier,
    resource::jsonb ->> 'resourceType' as resourceType,
    resource::jsonb ->> 'managingOrganization' as managingOrganization,
    resource::jsonb ->> 'participant' as participant,
_airbyte_extracted_at as _airbyte_emitted_at,
resource
from "airbyte"."airbyte"."care_team"


where _airbyte_extracted_at > (select coalesce(max(_airbyte_emitted_at),'1900-01-01') from "airbyte"."engage_analytics_engage_analytics_stg"."stg_careteams")
