{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_mood_rating_s3 with PII fields masked based on questionnaire_metadata.anon flag
-- Questionnaire: Mood Rating S3 (Questionnaire/207)
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
        mood_s3_on_a_scale_of_1_to_10_with_1_being_the_worst_mood_,
        mood_s3_total_score,
        CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_mood_rating_s3') }}