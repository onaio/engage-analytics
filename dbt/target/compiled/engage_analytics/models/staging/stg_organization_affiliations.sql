




select
    resource::jsonb ->> 'id' as id,
    resource::jsonb ->> 'meta' as meta,
    resource::jsonb ->> 'active' as active,
    resource::jsonb ->> 'resourceType' as resourceType,
    resource::jsonb ->> 'organization' as organization,
    resource::jsonb ->> 'identifier' as identifier,
    jsonb_array_elements(resource::jsonb -> 'location')::jsonb as location,
_airbyte_extracted_at as _airbyte_emitted_at
from "airbyte"."airbyte"."organization_affiliation"


where _airbyte_extracted_at > (select coalesce(max(_airbyte_emitted_at),'1900-01-01') from "airbyte"."engage_analytics_engage_analytics_stg"."stg_organization_affiliations")
