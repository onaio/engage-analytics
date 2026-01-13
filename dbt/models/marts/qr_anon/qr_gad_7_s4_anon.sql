{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_gad_7_s4
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
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_23 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_23,
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_24 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_24,
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_25 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_25,
    common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_5 as common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_5,
    common_mental_health_symptoms_not_being_able_to_stop_or_contr_5 as common_mental_health_symptoms_not_being_able_to_stop_or_contr_5,
    common_mental_health_symptoms_worrying_too_much_about_differe_5 as common_mental_health_symptoms_worrying_too_much_about_differe_5,
    common_mental_health_symptoms_trouble_relaxing_5 as common_mental_health_symptoms_trouble_relaxing_5,
    common_mental_health_symptoms_being_so_restless_that_it_s_har_5 as common_mental_health_symptoms_being_so_restless_that_it_s_har_5,
    common_mental_health_symptoms_becoming_easily_annoyed_or_irri_5 as common_mental_health_symptoms_becoming_easily_annoyed_or_irri_5,
    common_mental_health_symptoms_feeling_afraid_as_if_something__5 as common_mental_health_symptoms_feeling_afraid_as_if_something__5,
    CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_gad_7_s4') }}