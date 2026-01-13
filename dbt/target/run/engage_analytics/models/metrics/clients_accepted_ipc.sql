
  create view "airbyte"."engage_analytics"."clients_accepted_ipc__dbt_tmp"
    
    
  as (
    -- models/metrics/clients_accepted_ipc.sql


-- View to identify clients who accepted IPC (Interpersonal Counseling)
-- Based on Planning Next Steps questionnaire responses
-- When "planning_next_stepsinterpersonal_counseling_ipc_did_the_client_" is true, it indicates the client accepted IPC
-- Tracks cumulative patients who accepted IPC as of each date

with ipc_responses as (
  select
    qr.subject_patient_id,
    qr.author_practitioner_id,
    qr.meta_lastupdated::date as acceptance_date,
    pns.planning_next_stepsinterpersonal_counseling_ipc_did_the_client_,
    pns.practitioner_organization_id as organization_id
  from "airbyte"."engage_analytics_engage_analytics_stg"."stg_questionnaire_response" qr
  join "airbyte"."engage_analytics_engage_analytics_mart"."qr_planning_next_steps" pns
    on pns.qr_id = qr.id
  where qr.questionnaire_id = 'Questionnaire/q-planning-next-steps'
    and qr.subject_patient_id is not null
    and pns.planning_next_stepsinterpersonal_counseling_ipc_did_the_client_ = 'true'
),

-- Generate a date spine from the earliest acceptance to today
date_spine as (
  select generate_series(
    (select min(acceptance_date) from ipc_responses),
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
    p.acceptance_date
  from date_spine d
  cross join ipc_responses p
  where p.acceptance_date <= d.period_date
)

select
  period_date,
  organization_id,
  subject_patient_id,
  acceptance_date,
  true as has_accepted_ipc
from daily_counts
  );