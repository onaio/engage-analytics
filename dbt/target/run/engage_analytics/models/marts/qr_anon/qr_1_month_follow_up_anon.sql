
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_1_month_follow_up_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_1_month_follow_up with PII fields masked based on questionnaire_metadata.anon flag
-- Questionnaire: 1 Month Follow Up (Questionnaire/1-month-follow-up)
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
        "completed-all-recommended-interventions",
        'REDACTED' as "schedule-this-meeting",
        CURRENT_TIMESTAMP as anonymized_at

from "airbyte"."engage_analytics_engage_analytics_mart"."qr_1_month_follow_up"
  );