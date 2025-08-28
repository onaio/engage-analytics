
  create view "data_warehouse"."engage_analytics_mart"."fact_qr_latest_scores__dbt_tmp"
    
    
  as (
    -- Example: compute latest numeric totals per questionnaire (e.g., PHQ-9 / GAD-7 / PCL-5 8)
-- Assumes numeric answers; if coded answers are used, create a mapping table and join to resolve scores.
with scored as (
  select
    patient_id,
    questionnaire_canonical,
    authored_ts,
    sum(coalesce(value_num, 0)) as total_score
  from "data_warehouse"."engage_analytics_int"."int_qr_answers_long"
  group by 1,2,3
),
latest as (
  select distinct on (patient_id, questionnaire_canonical)
    patient_id,
    questionnaire_canonical,
    authored_ts,
    total_score
  from scored
  order by patient_id, questionnaire_canonical, authored_ts desc
)
select * from latest
  );