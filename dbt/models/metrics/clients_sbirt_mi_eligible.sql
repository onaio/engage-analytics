-- models/metrics/clients_sbirt_mi_eligible.sql
{{
    config(
        materialized='view'
    )
}}

-- View to identify clients eligible for SBIRT/MI (Screening, Brief Intervention, Referral to Treatment / Motivational Interviewing)
-- Based on Mental Wellness Tool (questionnaire 1613532) responses
-- When "alcohol-problem" or "drug-problem" is true, it indicates eligibility for SBIRT/MI
-- Tracks cumulative patients eligible for SBIRT/MI as of each date

with mw_tool_responses as (
  select
    qr.subject_patient_id,
    qr.author_practitioner_id,
    qr.meta_lastupdated::date as assessment_date,
    -- Extract the alcohol-problem boolean value from JSON
    (jsonb_path_query_first(
      qr.items::jsonb, 
      '$.**.item[*] ? (@.linkId == "alcohol-problem").answer[0].valueBoolean'
    ))::boolean as alcohol_problem,
    -- Extract the drug-problem boolean value from JSON
    (jsonb_path_query_first(
      qr.items::jsonb, 
      '$.**.item[*] ? (@.linkId == "drug-problem").answer[0].valueBoolean'
    ))::boolean as drug_problem
  from {{ ref('stg_questionnaire_response') }} qr
  where qr.questionnaire_id = 'Questionnaire/1613532'
    and qr.subject_patient_id is not null
),

patients_eligible_for_sbirt_mi as (
  select
    mw.subject_patient_id,
    mw.author_practitioner_id,
    mw.assessment_date,
    mw.alcohol_problem,
    mw.drug_problem,
    p.practitioner_organization_id as organization_id
  from mw_tool_responses mw
  left join {{ ref('patient') }} p 
    on p.id = mw.subject_patient_id
  -- Select patients where alcohol_problem or drug_problem is true (they are eligible for SBIRT/MI)
  where mw.alcohol_problem = true or mw.drug_problem = true
),

-- Generate a date spine from the earliest assessment to today
date_spine as (
  select generate_series(
    (select min(assessment_date) from patients_eligible_for_sbirt_mi),
    current_date,
    '1 day'::interval
  )::date as period_date
),

-- For each date, count cumulative patients assessed as eligible for SBIRT/MI up to that date
daily_counts as (
  select
    d.period_date,
    p.organization_id,
    p.subject_patient_id,
    p.assessment_date,
    p.alcohol_problem,
    p.drug_problem
  from date_spine d
  cross join patients_eligible_for_sbirt_mi p
  where p.assessment_date <= d.period_date
)

select
  period_date,
  organization_id,
  subject_patient_id,
  assessment_date,
  alcohol_problem,
  drug_problem,
  true as is_eligible_for_sbirt_mi
from daily_counts