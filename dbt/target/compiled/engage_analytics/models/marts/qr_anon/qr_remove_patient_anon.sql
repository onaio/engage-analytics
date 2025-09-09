

-- Anonymized view for qr_remove_patient
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
    ecbis_remove_family as ecbis_remove_family,
    ecbis_remove_family_2 as ecbis_remove_family_2,
    ecbis_remove_family_3 as ecbis_remove_family_3,
    ecbis_remove_family_4 as ecbis_remove_family_4,
    ecbis_remove_family_5 as ecbis_remove_family_5,
    survey_response as survey_response,
    ecbis_remove_family_7 as ecbis_remove_family_7,
    'REDACTED' as patient_birthdate,
    CURRENT_TIMESTAMP as anonymized_at

from "airbyte"."engage_analytics_engage_analytics_mart"."qr_remove_patient"