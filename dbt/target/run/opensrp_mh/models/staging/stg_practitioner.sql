
  create view "data_warehouse"."engage_analytics_stg"."stg_practitioner__dbt_tmp"
    
    
  as (
    select
    id as fhir_id,
    resource,
    resource ->> 'id'                            as resource_id,
    resource -> 'name' -> 0 ->> 'family'         as family_name,
    resource -> 'name' -> 0 -> 'given' ->> 0     as given_name,
    resource -> 'telecom' -> 0 ->> 'value'       as phone_raw,
    
    -- Handle ISO strings from FHIR meta/fields safely
    (lastUpdated::timestamptz)
          as lastUpdated_ts,
    _airbyte_extracted_at
from "data_warehouse"."airbyte"."practitioner"
  );