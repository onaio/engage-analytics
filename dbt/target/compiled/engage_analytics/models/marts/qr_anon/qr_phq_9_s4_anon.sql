

-- Anonymized view for qr_phq_9_s4 with PII fields masked based on questionnaire_metadata.anon flag
-- Questionnaire: PHQ-9 (IPC Session 4) (Questionnaire/211)
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
        ipc_s4_total_score,
        ipc_s4_how_difficult_have_these_problems_made_it_for_you_,
        'REDACTED' as ipc_s4_q8_movement,
        'REDACTED' as ipc_s4_q9_suicide,
        ipc_s4_q1_interest,
        ipc_s4_q2_depressed,
        ipc_s4_q3_sleep,
        'REDACTED' as ipc_s4_q4_tired,
        'REDACTED' as ipc_s4_q5_appetite,
        ipc_s4_q6_failure,
        'REDACTED' as ipc_s4_q7_concentration,
        ipc_s4_score_meaning,
        ipc_s4_score_meaning_13,
        CURRENT_TIMESTAMP as anonymized_at

from "airbyte"."engage_analytics_engage_analytics_mart"."qr_phq_9_s4"