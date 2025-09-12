
  create view "airbyte"."engage_analytics"."clients_eligible_referral__dbt_tmp"
    
    
  as (
    -- models/metrics/clients_eligible_referral.sql


-- View to identify clients eligible for referral
-- Based on Mental Wellness Tool (questionnaire 1613532) responses
-- When "severe-mental-health" is true, it indicates eligibility for referral
-- Tracks cumulative patients eligible for referral as of each date

with mw_tool_responses as (
  select
    qr.subject_patient_id,
    qr.author_practitioner_id,
    qr.meta_lastupdated::date as assessment_date,
    -- Extract the severe-mental-health boolean value from JSON
    (jsonb_path_query_first(
      qr.items::jsonb, 
      '$.**.item[*] ? (@.linkId == "severe-mental-health").answer[0].valueBoolean'
    ))::boolean as severe_mental_health
  from "airbyte"."engage_analytics_engage_analytics_stg"."stg_questionnaire_response" qr
  where qr.questionnaire_id = 'Questionnaire/1613532'
    and qr.subject_patient_id is not null
),

patients_eligible_for_referral as (
  select
    mw.subject_patient_id,
    mw.author_practitioner_id,
    mw.assessment_date,
    mw.severe_mental_health,
    p.practitioner_organization_id as organization_id
  from mw_tool_responses mw
  left join "airbyte"."engage_analytics_engage_analytics_mart"."patient" p 
    on p.id = mw.subject_patient_id
  -- Select patients where severe_mental_health is true (they are eligible for referral)
  where mw.severe_mental_health = true
),

-- Generate a date spine from the earliest assessment to today
date_spine as (
  select generate_series(
    (select min(assessment_date) from patients_eligible_for_referral),
    current_date,
    '1 day'::interval
  )::date as period_date
),

-- For each date, count cumulative patients assessed as eligible for referral up to that date
daily_counts as (
  select
    d.period_date,
    p.organization_id,
    p.subject_patient_id,
    p.assessment_date,
    p.severe_mental_health
  from date_spine d
  cross join patients_eligible_for_referral p
  where p.assessment_date <= d.period_date
)

select
  period_date,
  organization_id,
  subject_patient_id,
  assessment_date,
  severe_mental_health,
  true as is_eligible_for_referral
from daily_counts
  );