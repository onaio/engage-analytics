-- ABOUTME: Links mwTool screening eligibility to intervention initiation
-- ABOUTME: Supports indicator #14 (mwTool outcome and intervention initiated)

{{ config(materialized='view') }}

-- Clients eligible for each intervention based on mwTool screening
with ipc_eligible as (
    select distinct
        subject_patient_id,
        organization_id,
        'IPC' as intervention_type
    from {{ ref('clients_eligible_for_ipc') }}
),

sbirt_eligible as (
    select distinct
        subject_patient_id,
        organization_id,
        'SBIRT' as intervention_type
    from {{ ref('clients_sbirt_mi_eligible') }}
),

spi_eligible as (
    select distinct
        subject_patient_id,
        organization_id,
        'SPI' as intervention_type
    from {{ ref('clients_eligible_spi') }}
),

-- Clients who actually started each intervention
ipc_started as (
    select distinct subject_patient_id
    from {{ ref('qr_start_ipc_s1') }}
    where subject_patient_id is not null
),

sbirt_started as (
    select distinct subject_patient_id
    from {{ ref('qr_sbirt') }}
    where subject_patient_id is not null
),

spi_started as (
    select distinct subject_patient_id
    from {{ ref('qr_spi_subform_1') }}
    where subject_patient_id is not null
),

-- Combine eligibility with initiation status
combined as (
    select
        e.subject_patient_id,
        e.organization_id,
        e.intervention_type,
        s.subject_patient_id is not null as initiated_treatment
    from ipc_eligible e
    left join ipc_started s on s.subject_patient_id = e.subject_patient_id

    union all

    select
        e.subject_patient_id,
        e.organization_id,
        e.intervention_type,
        s.subject_patient_id is not null as initiated_treatment
    from sbirt_eligible e
    left join sbirt_started s on s.subject_patient_id = e.subject_patient_id

    union all

    select
        e.subject_patient_id,
        e.organization_id,
        e.intervention_type,
        s.subject_patient_id is not null as initiated_treatment
    from spi_eligible e
    left join spi_started s on s.subject_patient_id = e.subject_patient_id
)

select
    subject_patient_id,
    organization_id,
    intervention_type,
    initiated_treatment
from combined
