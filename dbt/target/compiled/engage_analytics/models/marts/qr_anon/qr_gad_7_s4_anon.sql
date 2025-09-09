

-- Anonymized view for qr_gad_7_s4 with PII fields masked based on questionnaire_metadata.anon flag
-- Questionnaire: Gad 7 S4 (Questionnaire/62)
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
        ipc_s4_q1_anxious,
        ipc_s4_q2_control_worry,
        ipc_s4_q3_worry_too_much,
        ipc_s4_q4_trouble_relaxing,
        ipc_s4_q5_restless,
        ipc_s4_q6_irritable,
        ipc_s4_q7_afraid,
        ipc_s4_total_score,
        ipc_s4_score_meaning,
        ipc_s4_score_meaning_10,
        CURRENT_TIMESTAMP as anonymized_at

from "airbyte"."engage_analytics_engage_analytics_mart"."qr_gad_7_s4"