
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_spi_subform_3_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_spi_subform_3
-- Automatically generated based on questionnaire_metadata.csv

select 
    questionnaire_id as questionnaire_id,
    subject_patient_id as subject_patient_id,
    encounter_id as encounter_id,
    author_practitioner_id as author_practitioner_id,
    practitioner_location_id as practitioner_location_id,
    practitioner_organization_id as practitioner_organization_id,
    practitioner_id as practitioner_id,
    practitioner_careteam_id as practitioner_careteam_id,
    application_version as application_version,
    qr_id as qr_id,
    place_declare_values as place_declare_values,
    CURRENT_TIMESTAMP as anonymized_at

from "airbyte"."engage_analytics_engage_analytics_mart"."qr_spi_subform_3"
  );