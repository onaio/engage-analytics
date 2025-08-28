
  create view "data_warehouse"."engage_analytics_mart"."fact_encounter__dbt_tmp"
    
    
  as (
    select
  e.fhir_id as encounter_id,
  e.patient_id,
  e.practitioner_id,
  e.status,
  e.class_code,
  e.type_code,
  e.type_display,
  e.period_start,
  e.period_end
from "data_warehouse"."engage_analytics_stg"."stg_encounter" e
  );