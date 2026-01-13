{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_sf_pcl_5_s3
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
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__25 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__25,
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__26 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__26,
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__27 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__27,
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__28 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__28,
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__29 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__29,
    pcl_5_item_7 as pcl_5_item_7,
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__30 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__30,
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_56 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_56,
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_57 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_57,
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__31 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__31,
    pcl_5_item_8 as pcl_5_item_8,
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_58 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_58,
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__32 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__32,
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_59 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_59,
    form_f1_question_26_3 as form_f1_question_26_3,
    form_f1_question_27_3 as form_f1_question_27_3,
    form_f1_question_28_3 as form_f1_question_28_3,
    form_f1_question_29_3 as form_f1_question_29_3,
    form_f1_question_30_3 as form_f1_question_30_3,
    pcl_5_item_9 as pcl_5_item_9,
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_60 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_60,
    CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_sf_pcl_5_s3') }}