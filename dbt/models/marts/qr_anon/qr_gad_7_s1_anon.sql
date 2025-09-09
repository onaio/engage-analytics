{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_gad_7_s1 with PII fields masked based on questionnaire_metadata.anon flag
-- Questionnaire: GAD-7 (IPC Session 1) (Questionnaire/202)
-- PII fields masked: 1 fields

select 
    MD5(COALESCE(qr_id, '')::text) as qr_id_hash,
    questionnaire_id,
    MD5(COALESCE(subject_patient_id, '')::text) as subject_patient_id_hash,
    encounter_id,
    author_practitioner_id,
    practitioner_location_id,
    practitioner_organization_id,
    practitioner_id,
    practitioner_careteam_id,
    application_version,
        ipc_s1_taskid_gad_pdf,
        ipc_s1_encounter_id_of_gad7_session_1,
        ipc_s1_q1_anxious,
        ipc_s1_q2_control_worry,
        ipc_s1_q3_worry_too_much,
        ipc_s1_q4_trouble_relaxing,
        ipc_s1_q5_restless,
        ipc_s1_q6_irritable,
        ipc_s1_q7_afraid,
        ipc_s1_observation_id_of_gad7_session_1_score,
        ipc_s1_total_score,
        ipc_s1_score_meaning,
        ipc_s1_score_meaning_13,
        'REDACTED' as date_of_birth,
        age,
        birth_month,
        age_years,
        encounter_reference,
        CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_gad_7_s1') }}