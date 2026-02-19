-- models/metrics/encounters_by_delivery_format.sql
{{
    config(
        materialized='view'
    )
}}

-- View to track daily encounters by delivery format
-- Based on IPC Session 1 questionnaire responses
-- Queries the long-format answer data directly by linkId to avoid
-- dependency on dynamically generated column names in wide views
-- Provides daily counts (not cumulative) for each delivery format type

with ipc_sessions as (
  select
    a.subject_patient_id,
    a.author_practitioner_id,
    a._airbyte_emitted_at::date as session_date,
    a.answer_value_text as format_you_deliver,
    t.practitioner_organization_id as organization_id
  from {{ ref('int_qr_answers_long') }} a
  left join {{ ref('int_qr_tags') }} t
    on t.resource_id = a.qr_id
  where a.questionnaire_id = 'Questionnaire/55'
    and a.linkid = 'acd9d89b-0d67-415e-a3a4-b132ab304c45'
    and a.subject_patient_id is not null
    and a.answer_value_text is not null
)

select
  session_date,
  organization_id,
  format_you_deliver,
  subject_patient_id,
  author_practitioner_id
from ipc_sessions
