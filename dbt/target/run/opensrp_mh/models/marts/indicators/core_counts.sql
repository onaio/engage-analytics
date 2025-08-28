
  create view "data_warehouse"."engage_analytics_mart"."core_counts__dbt_tmp"
    
    
  as (
    -- Basic counts / indicators
with a as (select count(*) as patients from "data_warehouse"."engage_analytics_mart"."dim_patient"),
b as (select count(*) as practitioners from "data_warehouse"."engage_analytics_mart"."dim_practitioner"),
c as (select count(*) as encounters from "data_warehouse"."engage_analytics_mart"."fact_encounter"),
d as (
    select practitioner_id, count(*) as encounters_per_practitioner
    from "data_warehouse"."engage_analytics_mart"."fact_encounter"
    group by 1
)
select
  a.patients,
  b.practitioners,
  c.encounters
from a,b,c
  );