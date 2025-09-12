-- models/metrics/clients_with_mental_health.sql
{{
    config(
        materialized='view'
    )
}}

-- View to identify clients with probable mental health issues
-- Based on Mental Wellness Tool (questionnaire 1613532) responses
-- When "no-mental-problem" is false, it indicates probable mental health issues
-- Tracks cumulative patients with mental health issues as of each date

with mw_tool_responses as (
  select
    qr.subject_patient_id,
    qr.author_practitioner_id,
    qr.meta_lastupdated::date as assessment_date,
    -- Extract the no-mental-problem boolean value from JSON
    -- When this is false, the patient has probable mental health issues
    (jsonb_path_query_first(
      qr.items::jsonb, 
      '$.**.item[*] ? (@.linkId == "no-mental-problem").answer[0].valueBoolean'
    ))::boolean as no_mental_problem
  from {{ ref('stg_questionnaire_response') }} qr
  where qr.questionnaire_id = 'Questionnaire/1613532'
    and qr.subject_patient_id is not null
),

patients_with_mental_health as (
  select
    mw.subject_patient_id,
    mw.author_practitioner_id,
    mw.assessment_date,
    mw.no_mental_problem,
    p.practitioner_organization_id as organization_id
  from mw_tool_responses mw
  left join {{ ref('patient') }} p 
    on p.id = mw.subject_patient_id
  -- Select patients where no_mental_problem is false (they have probable mental health issues)
  where mw.no_mental_problem = false
),

-- Generate a date spine from the earliest assessment to today
date_spine as (
  select generate_series(
    (select min(assessment_date) from patients_with_mental_health),
    current_date,
    '1 day'::interval
  )::date as period_date
),

-- For each date, count cumulative patients assessed with mental health issues up to that date
daily_counts as (
  select
    d.period_date,
    p.organization_id,
    p.subject_patient_id,
    p.assessment_date,
    p.no_mental_problem
  from date_spine d
  cross join patients_with_mental_health p
  where p.assessment_date <= d.period_date
)

select
  period_date,
  organization_id,
  subject_patient_id,
  assessment_date,
  no_mental_problem,
  true as has_probable_mental_health
from daily_counts