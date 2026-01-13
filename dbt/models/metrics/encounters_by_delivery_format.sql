-- models/metrics/encounters_by_delivery_format.sql
{{
    config(
        materialized='view'
    )
}}

-- View to track daily encounters by delivery format
-- Based on IPC Session 1 questionnaire responses
-- Provides daily counts (not cumulative) for each delivery format type

with ipc_sessions as (
  select
    qr.subject_patient_id,
    qr.author_practitioner_id,
    qr.meta_lastupdated::date as session_date,
    ipc.ipc_session_1_in_what_format_did_you_deliver_this_session_with_ as format_you_deliver,
    ipc.practitioner_organization_id as organization_id
  from {{ ref('stg_questionnaire_response') }} qr
  inner join {{ ref('qr_start_ipc_s1') }} ipc
    on ipc.qr_id = qr.id
  where qr.subject_patient_id is not null
    and ipc.ipc_session_1_in_what_format_did_you_deliver_this_session_with_ is not null
)

select
  session_date,
  organization_id,
  format_you_deliver,
  subject_patient_id,
  author_practitioner_id
from ipc_sessions