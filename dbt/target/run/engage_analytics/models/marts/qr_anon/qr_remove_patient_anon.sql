
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_remove_patient_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_remove_patient with PII fields masked based on questionnaire_metadata.anon flag
-- Questionnaire: eCBIS Remove Family Form (Questionnaire/69)
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
        remove_family_date_of_death,
        remove_family_give_other_reasons_for_removal,
        remove_family_age_at_death_year,
        remove_family_a11ebb0dacb34038956b293a41acb85b,
        remove_family_date_moved_away,
        remove_family_reason_for_removal,
        'REDACTED' as remove_family_patientbirthdate,
        reason_unknown,
        CURRENT_TIMESTAMP as anonymized_at

from "airbyte"."engage_analytics_engage_analytics_mart"."qr_remove_patient"
  );