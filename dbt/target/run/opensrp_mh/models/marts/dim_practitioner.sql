
  create view "data_warehouse"."engage_analytics_mart"."dim_practitioner__dbt_tmp"
    
    
  as (
    select
  pr.fhir_id as practitioner_id,
  coalesce(pr.given_name || ' ', '') || coalesce(pr.family_name,'') as practitioner_name,
  pr.phone_raw
from "data_warehouse"."engage_analytics_stg"."stg_practitioner" pr
  );