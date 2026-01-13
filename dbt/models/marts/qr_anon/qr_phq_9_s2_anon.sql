{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_phq_9_s2
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
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_32 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_32,
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_33 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_33,
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_34 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_34,
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_35 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_35,
    common_mental_health_symptoms_moving_or_speaking_so_slowly_th_3 as common_mental_health_symptoms_moving_or_speaking_so_slowly_th_3,
    common_mental_health_symptoms_thoughts_that_you_would_be_bett_3 as common_mental_health_symptoms_thoughts_that_you_would_be_bett_3,
    common_mental_health_symptoms_little_interest_or_pleasure_in__3 as common_mental_health_symptoms_little_interest_or_pleasure_in__3,
    common_mental_health_symptoms_feeling_down_depressed_or_hopel_3 as common_mental_health_symptoms_feeling_down_depressed_or_hopel_3,
    common_mental_health_symptoms_trouble_falling_or_staying_asle_3 as common_mental_health_symptoms_trouble_falling_or_staying_asle_3,
    common_mental_health_symptoms_feeling_tired_or_having_little__3 as common_mental_health_symptoms_feeling_tired_or_having_little__3,
    common_mental_health_symptoms_poor_appetite_or_overeating_3 as common_mental_health_symptoms_poor_appetite_or_overeating_3,
    common_mental_health_symptoms_feeling_bad_about_yourself_or_t_3 as common_mental_health_symptoms_feeling_bad_about_yourself_or_t_3,
    common_mental_health_symptoms_trouble_concentrating_on_things_3 as common_mental_health_symptoms_trouble_concentrating_on_things_3,
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_36 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_36,
    CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_phq_9_s2') }}