-- ABOUTME: Clients who received 1-month follow-up after intervention
-- ABOUTME: Supports indicator #29 (% follow-up)

{{ config(materialized='view') }}

-- Find 1-month follow-up tasks (code 040)
with follow_up_tasks as (
    select
        t.id as task_id,
        t.status,
        -- Extract patient ID from "for" field (format: Patient/uuid)
        replace((t.for::jsonb)->>'reference', 'Patient/', '') as subject_patient_id,
        -- Extract practitioner from owner
        replace((t.owner::jsonb)->>'reference', 'Practitioner/', '') as practitioner_id,
        (t.authoredon)::timestamp as task_created_date,
        (t.lastmodified)::timestamp as task_modified_date
    from {{ ref('stg_task') }} t
    where t.code::text like '%"040"%'
       and t.code::text like '%1 Month Follow Up%'
),

-- Join with practitioner to get organization
with_org as (
    select
        f.task_id,
        f.subject_patient_id,
        f.practitioner_id,
        f.status,
        f.task_created_date,
        f.task_modified_date,
        pr.organization_id
    from follow_up_tasks f
    left join {{ ref('practitioners') }} pr on pr.id = f.practitioner_id
)

select
    task_id,
    subject_patient_id,
    practitioner_id,
    organization_id,
    status,
    task_created_date,
    task_modified_date,
    status = 'completed' as follow_up_completed
from with_org
where subject_patient_id is not null
