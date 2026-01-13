
  create view "airbyte"."engage_analytics"."clients_eligible_fws__dbt_tmp"
    
    
  as (
    -- models/metrics/clients_eligible_fws.sql


-- View to identify clients eligible for FWS (Financial Wellness Services)
-- Based on Financial Wellness Tool (questionnaire q-financial-wellness-tool) responses
-- When "no-financial-hardship" is false, it indicates eligibility for FWS
-- Tracks cumulative patients eligible for FWS as of each date

with fw_tool_responses as (
  select
    qr.subject_patient_id,
    qr.author_practitioner_id,
    qr.meta_lastupdated::date as assessment_date,
    -- Extract the no-financial-hardship boolean value from JSON
    (jsonb_path_query_first(
      qr.items::jsonb, 
      '$.**.item[*] ? (@.linkId == "no-financial-hardship").answer[0].valueBoolean'
    ))::boolean as no_financial_hardship,
    p.practitioner_organization_id as organization_id
  from "airbyte"."engage_analytics_engage_analytics_stg"."stg_questionnaire_response" qr
  left join "airbyte"."engage_analytics_engage_analytics_mart"."patient" p 
    on p.id = qr.subject_patient_id
  where qr.questionnaire_id = 'Questionnaire/q-financial-wellness-tool'
    and qr.subject_patient_id is not null
),

patients_eligible_for_fws as (
  select
    subject_patient_id,
    author_practitioner_id,
    assessment_date,
    no_financial_hardship,
    organization_id
  from fw_tool_responses
  -- Select patients where no_financial_hardship is false (they have financial hardship and are eligible for FWS)
  where no_financial_hardship = false
),

-- Generate a date spine from the earliest assessment to today
date_spine as (
  select generate_series(
    (select min(assessment_date) from patients_eligible_for_fws),
    current_date,
    '1 day'::interval
  )::date as period_date
),

-- For each date, count cumulative patients assessed as eligible for FWS up to that date
daily_counts as (
  select
    d.period_date,
    p.organization_id,
    p.subject_patient_id,
    p.assessment_date,
    p.no_financial_hardship
  from date_spine d
  cross join patients_eligible_for_fws p
  where p.assessment_date <= d.period_date
)

select
  period_date,
  organization_id,
  subject_patient_id,
  assessment_date,
  no_financial_hardship,
  true as is_eligible_for_fws
from daily_counts
  );