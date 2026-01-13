{{
    config(
        materialized='view'
    )
}}

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
    ecbis_remove_family_form_date_of_death as ecbis_remove_family_form_date_of_death,
    ecbis_remove_family_form_give_other_reasons_for_removal as ecbis_remove_family_form_give_other_reasons_for_removal,
    ecbis_remove_family_form_age_at_death_year as ecbis_remove_family_form_age_at_death_year,
    ecbis_remove_family_form as ecbis_remove_family_form,
    ecbis_remove_family_form_date_moved_away as ecbis_remove_family_form_date_moved_away,
    survey_response as survey_response,
    ecbis_remove_family_form_reason_for_removal as ecbis_remove_family_form_reason_for_removal,
    'REDACTED' as patient_birthdate,
    CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_remove_patient') }}