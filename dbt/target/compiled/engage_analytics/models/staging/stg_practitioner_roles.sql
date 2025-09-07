




select
    resource::jsonb ->> 'id' as id,
    resource::jsonb ->> 'meta' as meta,
    resource::jsonb ->> 'active' as active,
    resource::jsonb ->> 'resourceType' as resourceType,
    resource::jsonb ->> 'organization' as organization,
    resource::jsonb ->> 'practitioner' as practitioner,
    resource::jsonb ->> 'code' as code,
_airbyte_extracted_at as _airbyte_emitted_at
from "airbyte"."airbyte"."practitioner_role"


where _airbyte_extracted_at > (select coalesce(max(_airbyte_emitted_at),'1900-01-01') from "airbyte"."engage_analytics_engage_analytics_stg"."stg_practitioner_roles")
