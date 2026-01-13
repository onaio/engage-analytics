




select
    resource::jsonb ->> 'id' as id,
    resource::jsonb ->> 'meta' as meta,
    resource::jsonb ->> 'subject' as subject,
    resource::jsonb ->> 'serviceType' as serviceType,
    resource::jsonb ->> 'resourceType' as resourceType,
    resource::jsonb ->> 'priority' as priority,
    resource::jsonb ->> 'class' as class,
    resource::jsonb ->> 'serviceProvider' as serviceProvider,
    resource::jsonb ->> 'reasonCode' as reasonCode,
    resource::jsonb ->> 'participant' as participant,
    resource::jsonb ->> 'type' as type,
    resource::jsonb ->> 'period' as period,
    resource::jsonb ->> 'status' as status,
_airbyte_extracted_at as _airbyte_emitted_at
from "airbyte"."airbyte"."encounter"


  where _airbyte_extracted_at > (select max(_airbyte_emitted_at) from "airbyte"."engage_analytics_engage_analytics_stg"."stg_encounter")
