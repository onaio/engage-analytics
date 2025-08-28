
  create view "data_warehouse"."engage_analytics_mart"."dim_patient__dbt_tmp"
    
    
  as (
    select
  p.fhir_id as patient_id,
  p.gender,
  (p.birth_date)::date as birth_date,
  (date_part('year', age((p.birth_date)::date)))::int as age_years,
  (p.active)::boolean as is_active
from "data_warehouse"."engage_analytics_stg"."stg_patient" p
  );