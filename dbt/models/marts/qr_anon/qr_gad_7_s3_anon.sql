{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_gad_7_s3 with PII fields masked based on questionnaire_metadata.anon flag
-- Questionnaire: Gad 7 S3 (Questionnaire/59)
-- PII fields masked: 0 fields

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
        ipc_s3_q1_anxious,
        ipc_s3_q2_control_worry,
        ipc_s3_q3_worry_too_much,
        ipc_s3_q4_trouble_relaxing,
        ipc_s3_q5_restless,
        ipc_s3_q6_irritable,
        ipc_s3_q7_afraid,
        ipc_s3_total_score,
        ipc_s3_score_meaning,
        ipc_s3_score_meaning_10,
        CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_gad_7_s3') }}