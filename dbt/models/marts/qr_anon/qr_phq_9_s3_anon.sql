{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_phq_9_s3 with PII fields masked based on questionnaire_metadata.anon flag
-- Questionnaire: PHQ-9 (IPC Session 3) (Questionnaire/61)
-- PII fields masked: 5 fields

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
        ipc_s3_total_score,
        'REDACTED' as ipc_s3_q8_movement,
        'REDACTED' as ipc_s3_q9_suicide,
        ipc_s3_q1_interest,
        ipc_s3_q2_depressed,
        ipc_s3_q3_sleep,
        'REDACTED' as ipc_s3_q4_tired,
        'REDACTED' as ipc_s3_q5_appetite,
        ipc_s3_q6_failure,
        'REDACTED' as ipc_s3_q7_concentration,
        ipc_s3_score_meaning,
        ipc_s3_score_meaning_12,
        CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_phq_9_s3') }}