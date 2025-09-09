

-- Anonymized view for qr_gad_7_s2 with PII fields masked based on questionnaire_metadata.anon flag
-- Questionnaire: Gad 7 S2 (Questionnaire/205)
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
        ipc_s2_encounter_id_of_gad7_session_2,
        ipc_s2_q1_anxious,
        ipc_s2_q2_control_worry,
        ipc_s2_q3_worry_too_much,
        ipc_s2_q4_trouble_relaxing,
        ipc_s2_q5_restless,
        ipc_s2_q6_irritable,
        ipc_s2_q7_afraid,
        ipc_s2_observation_id_of_gad7_session_2_score,
        ipc_s2_total_score,
        ipc_s2_score_meaning,
        ipc_s2_score_meaning_12,
        CURRENT_TIMESTAMP as anonymized_at

from "airbyte"."engage_analytics_engage_analytics_mart"."qr_gad_7_s2"