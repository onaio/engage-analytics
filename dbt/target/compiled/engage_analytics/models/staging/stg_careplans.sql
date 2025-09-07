




select
    resource::jsonb ->> 'id' as id,
    resource::jsonb ->> 'meta' as meta,
    resource::jsonb ->> 'subject' as subject,
    resource::jsonb ->> 'title' as title,
    resource::jsonb ->> 'author' as author,
    resource::jsonb ->> 'description' as description,
    resource::jsonb ->> 'resourceType' as resourceType,
    resource::jsonb ->> 'intent' as intent,
    resource::jsonb ->> 'created' as created,
    resource::jsonb ->> 'period' as period,
    resource::jsonb ->> 'identifier' as identifier,
    resource::jsonb ->> 'status' as status,
    resource::jsonb ->> 'activity' as activity,
    resource::jsonb ->> 'instantiatesCanonical' as instantiatesCanonical,
_airbyte_extracted_at as _airbyte_emitted_at
from "airbyte"."airbyte"."care_plan"


where _airbyte_extracted_at > (select coalesce(max(_airbyte_emitted_at),'1900-01-01') from "airbyte"."engage_analytics_engage_analytics_stg"."stg_careplans")
