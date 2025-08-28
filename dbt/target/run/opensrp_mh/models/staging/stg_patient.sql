
  create view "data_warehouse"."engage_analytics_stg"."stg_patient__dbt_tmp"
    
    
  as (
    with src as (
    select
        id as fhir_id,
        resource,
        resource ->> 'id'                  as resource_id,
        resource -> 'meta' ->> 'versionId' as version_id,
        resource -> 'meta' ->> 'lastUpdated' as meta_last_updated,
        resource ->> 'gender'              as gender,
        resource ->> 'birthDate'           as birth_date,
        resource ->> 'active'              as active,

        
  (nullif("lastUpdated", '')::timestamptz)
 as lastUpdated_ts,   -- << quote the column
        _airbyte_extracted_at
    from "data_warehouse"."airbyte"."patient"
)
select * from src
  );