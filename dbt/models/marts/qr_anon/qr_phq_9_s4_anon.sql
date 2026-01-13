{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_phq_9_s4
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
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_40 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_40,
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_41 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_41,
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_42 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_42,
    phq_9_ipc_session_2_how_difficult_have_these_problems_made_it_3 as phq_9_ipc_session_2_how_difficult_have_these_problems_made_it_3,
    common_mental_health_symptoms_moving_or_speaking_so_slowly_th_5 as common_mental_health_symptoms_moving_or_speaking_so_slowly_th_5,
    common_mental_health_symptoms_thoughts_that_you_would_be_bett_5 as common_mental_health_symptoms_thoughts_that_you_would_be_bett_5,
    common_mental_health_symptoms_little_interest_or_pleasure_in__5 as common_mental_health_symptoms_little_interest_or_pleasure_in__5,
    common_mental_health_symptoms_feeling_down_depressed_or_hopel_5 as common_mental_health_symptoms_feeling_down_depressed_or_hopel_5,
    common_mental_health_symptoms_trouble_falling_or_staying_asle_5 as common_mental_health_symptoms_trouble_falling_or_staying_asle_5,
    common_mental_health_symptoms_feeling_tired_or_having_little__5 as common_mental_health_symptoms_feeling_tired_or_having_little__5,
    common_mental_health_symptoms_poor_appetite_or_overeating_5 as common_mental_health_symptoms_poor_appetite_or_overeating_5,
    common_mental_health_symptoms_feeling_bad_about_yourself_or_t_5 as common_mental_health_symptoms_feeling_bad_about_yourself_or_t_5,
    common_mental_health_symptoms_trouble_concentrating_on_things_5 as common_mental_health_symptoms_trouble_concentrating_on_things_5,
    CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_phq_9_s4') }}