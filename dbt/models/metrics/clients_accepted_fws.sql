-- models/metrics/clients_accepted_fws.sql
{{
    config(
        materialized='view'
    )
}}

-- View to identify clients who accepted FWS (Financial Wellness Supports)
-- Based on Planning Next Steps questionnaire responses
-- When "planning_next_stepsfinancial_wellness_supports_did_the_client_a" is true, it indicates the client accepted FWS
-- Tracks cumulative patients who accepted FWS as of each date

with fws_responses as (
  select
    qr.subject_patient_id,
    qr.author_practitioner_id,
    qr.meta_lastupdated::date as acceptance_date,
    pns.planning_next_stepsfinancial_wellness_supports_did_the_client_a,
    pns.practitioner_organization_id as organization_id
  from {{ ref('stg_questionnaire_response') }} qr
  join {{ ref('qr_planning_next_steps') }} pns
    on pns.qr_id = qr.id
  where qr.questionnaire_id = 'Questionnaire/q-planning-next-steps'
    and qr.subject_patient_id is not null
    and pns.planning_next_stepsfinancial_wellness_supports_did_the_client_a = 'true'
),

-- Generate a date spine from the earliest acceptance to today
date_spine as (
  select generate_series(
    (select min(acceptance_date) from fws_responses),
    current_date,
    '1 day'::interval
  )::date as period_date
),

-- For each date, count cumulative patients who accepted FWS up to that date
daily_counts as (
  select
    d.period_date,
    p.organization_id,
    p.subject_patient_id,
    p.acceptance_date
  from date_spine d
  cross join fws_responses p
  where p.acceptance_date <= d.period_date
)

select
  period_date,
  organization_id,
  subject_patient_id,
  acceptance_date,
  true as has_accepted_fws
from daily_counts
