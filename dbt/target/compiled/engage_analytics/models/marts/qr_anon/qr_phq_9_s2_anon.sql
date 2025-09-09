

-- Anonymized view for qr_phq_9_s2 with PII fields masked based on questionnaire_metadata.anon flag
-- Questionnaire: PHQ-9 (IPC Session 2) (Questionnaire/57)
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
        ipc_s2_encounter_id_of_phq9_session_2,
        'REDACTED' as ipc_s2_q8_movement,
        'REDACTED' as ipc_s2_q9_suicide,
        ipc_s2_q1_interest,
        ipc_s2_q2_depressed,
        ipc_s2_q3_sleep,
        'REDACTED' as ipc_s2_q4_tired,
        'REDACTED' as ipc_s2_q5_appetite,
        ipc_s2_q6_failure,
        'REDACTED' as ipc_s2_q7_concentration,
        ipc_s2_observation_id_of_phq9_session_2_score,
        ipc_s2_total_score,
        ipc_s2_score_meaning,
        ipc_s2_score_meaning_14,
        CURRENT_TIMESTAMP as anonymized_at

from "airbyte"."engage_analytics_engage_analytics_mart"."qr_phq_9_s2"