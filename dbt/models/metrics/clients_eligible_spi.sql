-- models/metrics/clients_eligible_spi.sql
{{
    config(
        materialized='view'
    )
}}

-- View to identify clients eligible for SPI (Suicide Prevention Intervention)
-- Based on Mental Wellness Tool (questionnaire 1613532) responses
-- When "suicide-risk" is true, it indicates eligibility for SPI
-- Tracks cumulative patients eligible for SPI as of each date

with mw_tool_responses as (
  select
    qr.subject_patient_id,
    qr.author_practitioner_id,
    qr.meta_lastupdated::date as assessment_date,
    -- Extract the suicide-risk boolean value from JSON
    (jsonb_path_query_first(
      qr.items::jsonb, 
      '$.**.item[*] ? (@.linkId == "suicide-risk").answer[0].valueBoolean'
    ))::boolean as suicide_risk
  from {{ ref('stg_questionnaire_response') }} qr
  where qr.questionnaire_id = 'Questionnaire/1613532'
    and qr.subject_patient_id is not null
),

patients_eligible_for_spi as (
  select
    mw.subject_patient_id,
    mw.author_practitioner_id,
    mw.assessment_date,
    mw.suicide_risk,
    p.practitioner_organization_id as organization_id
  from mw_tool_responses mw
  left join {{ ref('patient') }} p 
    on p.id = mw.subject_patient_id
  -- Select patients where suicide_risk is true (they are eligible for SPI)
  where mw.suicide_risk = true
),

-- Generate a date spine from the earliest assessment to today
date_spine as (
  select generate_series(
    (select min(assessment_date) from patients_eligible_for_spi),
    current_date,
    '1 day'::interval
  )::date as period_date
),

-- For each date, count cumulative patients assessed as eligible for SPI up to that date
daily_counts as (
  select
    d.period_date,
    p.organization_id,
    p.subject_patient_id,
    p.assessment_date,
    p.suicide_risk
  from date_spine d
  cross join patients_eligible_for_spi p
  where p.assessment_date <= d.period_date
)

select
  period_date,
  organization_id,
  subject_patient_id,
  assessment_date,
  suicide_risk,
  true as is_eligible_for_spi
from daily_counts