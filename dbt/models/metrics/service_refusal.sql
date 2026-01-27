-- ABOUTME: Tracks service acceptance and refusal from Planning Next Steps
-- ABOUTME: Supports indicator #20 (Refusal rate)

{{ config(materialized='view') }}

select
    subject_patient_id,
    practitioner_organization_id as organization_id,
    -- IPC
    planning_next_stepsinterpersonal_counseling_ipc_did_the_client_ = 'true' as accepted_ipc,
    planning_next_stepsinterpersonal_counseling_ipc_did_the_client_ = 'false' as declined_ipc,
    -- SBIRT
    planning_next_steps_did_the_client_accept_sbirt = 'true' as accepted_sbirt,
    planning_next_steps_did_the_client_accept_sbirt = 'false' as declined_sbirt,
    -- FWS
    planning_next_stepsfinancial_wellness_supports_did_the_client_a = 'true' as accepted_fws,
    planning_next_stepsfinancial_wellness_supports_did_the_client_a = 'false' as declined_fws,
    -- Referral
    planning_next_stepsreferral_to_mental_health_specialist_did_the = 'true' as accepted_referral,
    planning_next_stepsreferral_to_mental_health_specialist_did_the = 'false' as declined_referral,
    -- SPI
    planning_next_steps_did_the_client_accept_spi = 'true' as accepted_spi,
    planning_next_steps_did_the_client_accept_spi = 'false' as declined_spi
from {{ ref('qr_planning_next_steps') }}
where subject_patient_id is not null
