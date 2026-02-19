-- models/metrics/clients_accepted_sbirt.sql
{{
    config(
        materialized='view'
    )
}}

-- View to identify clients who accepted SBIRT (Screening, Brief Intervention, and Referral to Treatment)
-- Based on Planning Next Steps questionnaire responses
-- Queries the long-format answer data directly by linkId to avoid
-- dependency on dynamically generated column names in wide views
-- Tracks cumulative patients who accepted SBIRT as of each date

with sbirt_responses as (
  select
    a.subject_patient_id,
    a.author_practitioner_id,
    a._airbyte_emitted_at::date as acceptance_date,
    t.practitioner_organization_id as organization_id
  from {{ ref('int_qr_answers_long') }} a
  left join {{ ref('int_qr_tags') }} t
    on t.resource_id = a.qr_id
  where a.questionnaire_id = 'Questionnaire/q-planning-next-steps'
    and a.linkid = 'c9d8b7a6-f5e4-4d3c-b2a1-09f8e7d6c5b4'
    and a.answer_value_text = 'true'
    and a.subject_patient_id is not null
),

-- Generate a date spine from the earliest acceptance to today
date_spine as (
  select generate_series(
    (select min(acceptance_date) from sbirt_responses),
    current_date,
    '1 day'::interval
  )::date as period_date
),

-- For each date, count cumulative patients who accepted SBIRT up to that date
daily_counts as (
  select
    d.period_date,
    p.organization_id,
    p.subject_patient_id,
    p.acceptance_date
  from date_spine d
  cross join sbirt_responses p
  where p.acceptance_date <= d.period_date
)

select
  period_date,
  organization_id,
  subject_patient_id,
  acceptance_date,
  true as has_accepted_sbirt
from daily_counts
