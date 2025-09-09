
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_remove_patient_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_remove_patient with PII fields masked based on questionnaire_metadata.anon flag
-- Auto-generated to mask fields marked as anon=TRUE in questionnaire_metadata

with metadata_pii as (
    select distinct
        question_alias
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where questionnaire_id in ('Questionnaire/69')
    and anon = 'TRUE'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_remove_patient"
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
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'remove_family_date_of_death')
        THEN 'REDACTED'
        ELSE remove_family_date_of_death::text
    END as remove_family_date_of_death,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'remove_family_give_other_reasons_for_removal')
        THEN 'REDACTED'
        ELSE remove_family_give_other_reasons_for_removal::text
    END as remove_family_give_other_reasons_for_removal,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'remove_family_age_at_death_year')
        THEN 'REDACTED'
        ELSE remove_family_age_at_death_year::text
    END as remove_family_age_at_death_year,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'remove_family_a11ebb0dacb34038956b293a41acb85b')
        THEN 'REDACTED'
        ELSE remove_family_a11ebb0dacb34038956b293a41acb85b::text
    END as remove_family_a11ebb0dacb34038956b293a41acb85b,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'remove_family_date_moved_away')
        THEN 'REDACTED'
        ELSE remove_family_date_moved_away::text
    END as remove_family_date_moved_away,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'remove_family_reason_for_removal')
        THEN 'REDACTED'
        ELSE remove_family_reason_for_removal::text
    END as remove_family_reason_for_removal,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'remove_family_patientbirthdate')
        THEN 'REDACTED'
        ELSE remove_family_patientbirthdate::text
    END as remove_family_patientbirthdate,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'reason_unknown')
        THEN 'REDACTED'
        ELSE reason_unknown::text
    END as reason_unknown,
        CURRENT_TIMESTAMP as anonymized_at

from source_data
  );