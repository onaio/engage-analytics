



 
    
    select
    resource::jsonb ->> 'id' as id,
    resource::jsonb ->> 'meta' as meta,
    resource::jsonb ->> 'name' as name,
    resource::jsonb ->> 'active' as active,
    resource::jsonb ->> 'identifier' as identifier,
    resource::jsonb ->> 'resourceType' as resourceType,
    jsonb_array_elements(resource::jsonb -> 'type')::jsonb as type,
     _airbyte_extracted_at as _airbyte_emitted_at 

    from "airbyte"."airbyte"."organization"



  -- this filter will only be applied on an incremental run
   WHERE _airbyte_extracted_at > (SELECT max(_airbyte_emitted_at) FROM "airbyte"."engage_analytics_engage_analytics_stg"."stg_organizations")
