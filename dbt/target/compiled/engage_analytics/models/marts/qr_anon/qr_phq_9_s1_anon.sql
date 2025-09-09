

-- Anonymized view for qr_phq_9_s1 with PII fields masked based on questionnaire_metadata.anon flag
-- Questionnaire: PHQ-9 (IPC Session 1) (Questionnaire/54)
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
        phq9_q1_interest,
        phq9_q2_depressed,
        phq9_q3_sleep,
        'REDACTED' as phq9_q4_tired,
        'REDACTED' as phq9_q5_appetite,
        phq9_q6_failure,
        'REDACTED' as phq9_q7_concentration,
        'REDACTED' as phq9_q8_movement,
        'REDACTED' as phq9_q9_suicide,
        phq9_total_score,
        phq9_score_meaning,
        phq9_score_meaning_code,
        phq9_encounter_id,
        phq9_observation_id,
        phq9_calculated_month,
        phq9_calculated_year,
        phq9_field_cd8e3d6d,
        phq9_task_id,
        phq9_field_9c366bcc,
        phq9_field_b5bc7f80,
        phq9_link_encounter,
        CURRENT_TIMESTAMP as anonymized_at

from "airbyte"."engage_analytics_engage_analytics_mart"."qr_phq_9_s1"