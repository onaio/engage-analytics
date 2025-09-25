-- models/metrics/clients_accepted_ipc.sql


-- View to identify clients who accepted IPC (Integrated Primary Care)
-- Based on Planning Next Steps questionnaire responses
-- When "scheduled_ipc_session" is not null, it indicates the client accepted IPC
-- Tracks cumulative patients who accepted IPC as of each date

with planning_responses as (
  select
    qr.subject_patient_id,
    qr.author_practitioner_id,
    qr.meta_lastupdated::date as acceptance_date,
    pns.scheduled_ipc_session,
    pns.practitioner_organization_id as organization_id
  from "airbyte"."engage_analytics_engage_analytics_stg"."stg_questionnaire_response" qr
  join "airbyte"."engage_analytics_engage_analytics_mart"."qr_planning_next_steps" pns
    on pns.qr_id = qr.id
  where qr.questionnaire_id = 'Questionnaire/q-planning-next-steps'
    and qr.subject_patient_id is not null
    and pns.scheduled_ipc_session is not null
    and trim(pns.scheduled_ipc_session) != ''
),

-- Generate a date spine from the earliest acceptance to today
date_spine as (
  select generate_series(
    (select min(acceptance_date) from planning_responses),
    current_date,
    '1 day'::interval
  )::date as period_date
),

-- For each date, count cumulative patients who accepted IPC up to that date
daily_counts as (
  select
    d.period_date,
    p.organization_id,
    p.subject_patient_id,
    p.acceptance_date,
    p.scheduled_ipc_session
  from date_spine d
  cross join planning_responses p
  where p.acceptance_date <= d.period_date
)

select
  period_date,
  organization_id,
  subject_patient_id,
  acceptance_date,
  scheduled_ipc_session,
  true as has_accepted_ipc
from daily_counts