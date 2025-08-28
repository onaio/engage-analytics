-- Patients who have no encounter beyond (at most) 1 total encounter
with ec as (
  select patient_id, count(*) as encounter_count
  from "data_warehouse"."engage_analytics_mart"."fact_encounter"
  group by 1
)
select p.patient_id
from "data_warehouse"."engage_analytics_mart"."dim_patient" p
left join ec on ec.patient_id = p.patient_id
where coalesce(ec.encounter_count,0) <= 1