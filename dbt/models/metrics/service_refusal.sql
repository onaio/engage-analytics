-- ABOUTME: Tracks service acceptance and refusal from Planning Next Steps
-- ABOUTME: Supports indicator #20 (Refusal rate)
-- Queries the long-format answer data directly by linkId to avoid
-- dependency on dynamically generated column names in wide views

{{ config(materialized='view') }}

with answers as (
    select
        qr_id,
        subject_patient_id,
        linkid,
        answer_value_text
    from {{ ref('int_qr_answers_long') }}
    where questionnaire_id = 'Questionnaire/q-planning-next-steps'
        and subject_patient_id is not null
        and linkid in (
            'did-client-accept-any-ipc',
            'did-client-accept-any-sbirt',
            'did-client-accept-any-fws',
            'did-client-accept-any-mhs',
            'did-client-accept-any-spi'
        )
),

pivoted as (
    select
        qr_id,
        subject_patient_id,
        max(case when linkid = 'did-client-accept-any-ipc' then answer_value_text end) as ipc_response,
        max(case when linkid = 'did-client-accept-any-sbirt' then answer_value_text end) as sbirt_response,
        max(case when linkid = 'did-client-accept-any-fws' then answer_value_text end) as fws_response,
        max(case when linkid = 'did-client-accept-any-mhs' then answer_value_text end) as referral_response,
        max(case when linkid = 'did-client-accept-any-spi' then answer_value_text end) as spi_response
    from answers
    group by qr_id, subject_patient_id
)

select
    subject_patient_id,
    t.practitioner_organization_id as organization_id,
    -- IPC
    p.ipc_response = 'true' as accepted_ipc,
    p.ipc_response = 'false' as declined_ipc,
    -- SBIRT
    p.sbirt_response = 'true' as accepted_sbirt,
    p.sbirt_response = 'false' as declined_sbirt,
    -- FWS
    p.fws_response = 'true' as accepted_fws,
    p.fws_response = 'false' as declined_fws,
    -- Referral
    p.referral_response = 'true' as accepted_referral,
    p.referral_response = 'false' as declined_referral,
    -- SPI
    p.spi_response = 'true' as accepted_spi,
    p.spi_response = 'false' as declined_spi
from pivoted p
left join {{ ref('int_qr_tags') }} t
    on t.resource_id = p.qr_id
