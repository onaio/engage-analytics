
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_mood_rating_s1_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_mood_rating_s1 with PII fields masked based on questionnaire_metadata.anon flag
-- Questionnaire: Mood Rating (IPC Session 1) (Questionnaire/203)
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
        mood_s1_on_a_scale_of_1_to_10_with_1_being_the_worst_mood_,
        mood_s1_8085cded8e0f497f9e3d5fab3981d727,
        mood_s1_total_score,
        'REDACTED' as date_of_birth,
        age,
        birth_month,
        age_years,
        encounter_reference,
        CURRENT_TIMESTAMP as anonymized_at

from "airbyte"."engage_analytics_engage_analytics_mart"."qr_mood_rating_s1"
  );