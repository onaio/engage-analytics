
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_mood_rating_s4_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_mood_rating_s4 with PII fields masked based on questionnaire_metadata.anon flag
-- Auto-generated to mask fields marked as anon=TRUE in questionnaire_metadata

with metadata_pii as (
    select distinct
        question_alias
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where questionnaire_id in ('Questionnaire/209')
    and anon = 'TRUE'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_mood_rating_s4"
)

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
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'mood_s4_on_a_scale_of_1_to_10_with_1_being_the_worst_mood_')
        THEN 'REDACTED'
        ELSE mood_s4_on_a_scale_of_1_to_10_with_1_being_the_worst_mood_::text
    END as mood_s4_on_a_scale_of_1_to_10_with_1_being_the_worst_mood_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'mood_s4_total_score')
        THEN 'REDACTED'
        ELSE mood_s4_total_score::text
    END as mood_s4_total_score,
        CURRENT_TIMESTAMP as anonymized_at

from source_data
  );