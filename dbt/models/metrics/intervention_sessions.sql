-- ABOUTME: Aggregates counseling sessions by intervention type
-- ABOUTME: Supports indicator #7 (Number of counseling sessions)

{{ config(materialized='view') }}

-- IPC Sessions (S1-S4)
with ipc_sessions as (
    select
        subject_patient_id,
        practitioner_organization_id as organization_id,
        qr_id,
        'IPC' as intervention_type,
        1 as session_number
    from {{ ref('qr_start_ipc_s1') }}
    where subject_patient_id is not null

    union all

    select
        subject_patient_id,
        practitioner_organization_id as organization_id,
        qr_id,
        'IPC' as intervention_type,
        2 as session_number
    from {{ ref('qr_start_ipc_s2') }}
    where subject_patient_id is not null

    union all

    select
        subject_patient_id,
        practitioner_organization_id as organization_id,
        qr_id,
        'IPC' as intervention_type,
        3 as session_number
    from {{ ref('qr_start_ipc_s3') }}
    where subject_patient_id is not null

    union all

    select
        subject_patient_id,
        practitioner_organization_id as organization_id,
        qr_id,
        'IPC' as intervention_type,
        4 as session_number
    from {{ ref('qr_start_ipc_s4') }}
    where subject_patient_id is not null
),

-- SPI Sessions (S1-S4)
spi_sessions as (
    select
        subject_patient_id,
        practitioner_organization_id as organization_id,
        qr_id,
        'SPI' as intervention_type,
        1 as session_number
    from {{ ref('qr_spi_subform_1') }}
    where subject_patient_id is not null

    union all

    select
        subject_patient_id,
        practitioner_organization_id as organization_id,
        qr_id,
        'SPI' as intervention_type,
        2 as session_number
    from {{ ref('qr_spi_subform_2') }}
    where subject_patient_id is not null

    union all

    select
        subject_patient_id,
        practitioner_organization_id as organization_id,
        qr_id,
        'SPI' as intervention_type,
        3 as session_number
    from {{ ref('qr_spi_subform_3') }}
    where subject_patient_id is not null

    union all

    select
        subject_patient_id,
        practitioner_organization_id as organization_id,
        qr_id,
        'SPI' as intervention_type,
        4 as session_number
    from {{ ref('qr_spi_subform_4') }}
    where subject_patient_id is not null
),

-- SBIRT Sessions
sbirt_sessions as (
    select
        subject_patient_id,
        practitioner_organization_id as organization_id,
        qr_id,
        'SBIRT' as intervention_type,
        1 as session_number
    from {{ ref('qr_sbirt') }}
    where subject_patient_id is not null
),

-- Combine all sessions
all_sessions as (
    select * from ipc_sessions
    union all
    select * from spi_sessions
    union all
    select * from sbirt_sessions
)

select
    subject_patient_id,
    organization_id,
    qr_id,
    intervention_type,
    session_number
from all_sessions
