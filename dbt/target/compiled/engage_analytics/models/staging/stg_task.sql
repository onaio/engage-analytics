




select
    resource::jsonb ->> 'id' as id,
    resource::jsonb ->> 'meta' as meta,
    resource::jsonb ->> 'authoredOn' as authoredOn,
    resource::jsonb ->> 'basedOn' as basedOn,
    resource::jsonb ->> 'statusReason' as statusReason,
    resource::jsonb ->> 'reasonReference' as reasonReference,
    resource::jsonb ->> 'executionPeriod' as executionPeriod,
    resource::jsonb ->> 'for' as for,
    resource::jsonb ->> 'code' as code,
    resource::jsonb ->> 'owner' as owner,
    resource::jsonb ->> 'description' as description,
    resource::jsonb ->> 'resourceType' as resourceType,
    resource::jsonb ->> 'intent' as intent,
    resource::jsonb ->> 'priority' as priority,
    resource::jsonb ->> 'lastModified' as lastModified,
    resource::jsonb ->> 'requester' as requester,
    resource::jsonb ->> 'status' as status,
    resource::jsonb ->> 'reasonCode' as reasonCode,
_airbyte_extracted_at as _airbyte_emitted_at
from "airbyte"."airbyte"."task"


  where _airbyte_extracted_at > (select max(_airbyte_emitted_at) from "airbyte"."engage_analytics_engage_analytics_stg"."stg_task")
