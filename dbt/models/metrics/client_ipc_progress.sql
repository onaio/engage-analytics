-- ABOUTME: Tracks IPC session completion progress per client
-- ABOUTME: Used to calculate retention and completion rate metrics

{{
    config(
        materialized='view'
    )
}}

-- Track which IPC sessions each client has completed
with ipc_s1 as (
    select distinct
        subject_patient_id,
        practitioner_organization_id as organization_id,
        min(qr_id) as first_qr_id
    from {{ ref('qr_start_ipc_s1') }}
    where subject_patient_id is not null
    group by 1, 2
),

ipc_s2 as (
    select distinct subject_patient_id
    from {{ ref('qr_start_ipc_s2') }}
    where subject_patient_id is not null
),

ipc_s3 as (
    select distinct subject_patient_id
    from {{ ref('qr_start_ipc_s3') }}
    where subject_patient_id is not null
),

ipc_s4 as (
    select distinct subject_patient_id
    from {{ ref('qr_start_ipc_s4') }}
    where subject_patient_id is not null
),

client_progress as (
    select
        s1.subject_patient_id,
        s1.organization_id,
        true as completed_s1,
        s2.subject_patient_id is not null as completed_s2,
        s3.subject_patient_id is not null as completed_s3,
        s4.subject_patient_id is not null as completed_s4,
        (1 +
         case when s2.subject_patient_id is not null then 1 else 0 end +
         case when s3.subject_patient_id is not null then 1 else 0 end +
         case when s4.subject_patient_id is not null then 1 else 0 end
        ) as sessions_completed
    from ipc_s1 s1
    left join ipc_s2 s2 on s2.subject_patient_id = s1.subject_patient_id
    left join ipc_s3 s3 on s3.subject_patient_id = s1.subject_patient_id
    left join ipc_s4 s4 on s4.subject_patient_id = s1.subject_patient_id
)

select
    subject_patient_id,
    organization_id,
    completed_s1,
    completed_s2,
    completed_s3,
    completed_s4,
    sessions_completed,
    sessions_completed = 4 as completed_all_sessions
from client_progress
