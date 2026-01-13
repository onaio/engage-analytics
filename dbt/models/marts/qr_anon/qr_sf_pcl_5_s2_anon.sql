{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_sf_pcl_5_s2
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
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__17 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__17,
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__18 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__18,
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__19 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__19,
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__20 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__20,
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__21 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__21,
    pcl_5_item_4 as pcl_5_item_4,
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_50 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_50,
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__22 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__22,
    pcl_5_item_5 as pcl_5_item_5,
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_51 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_51,
    pcl_5_item_6 as pcl_5_item_6,
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__23 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__23,
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_52 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_52,
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__24 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__24,
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_53 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_53,
    form_f1_question_26_2 as form_f1_question_26_2,
    form_f1_question_27_2 as form_f1_question_27_2,
    form_f1_question_28_2 as form_f1_question_28_2,
    form_f1_question_29_2 as form_f1_question_29_2,
    form_f1_question_30_2 as form_f1_question_30_2,
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_54 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_54,
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_55 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_55,
    CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_sf_pcl_5_s2') }}