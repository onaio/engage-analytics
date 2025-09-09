

-- Anonymized view for qr_mood_rating_s4 with PII fields masked based on questionnaire_metadata.anon flag
-- Questionnaire: Mood Rating S4 (Questionnaire/209)
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
        mood_s4_on_a_scale_of_1_to_10_with_1_being_the_worst_mood_,
        mood_s4_total_score,
        CURRENT_TIMESTAMP as anonymized_at

from "airbyte"."engage_analytics_engage_analytics_mart"."qr_mood_rating_s4"