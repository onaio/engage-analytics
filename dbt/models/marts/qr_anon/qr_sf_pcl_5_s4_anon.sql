{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_sf_pcl_5_s4
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
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__33 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__33,
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__34 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__34,
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__35 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__35,
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_61 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_61,
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__36 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__36,
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__37 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__37,
    pcl_5_item_10 as pcl_5_item_10,
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__38 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__38,
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_62 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_62,
    pcl_5_item_11 as pcl_5_item_11,
    pcl_5_item_12 as pcl_5_item_12,
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__39 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__39,
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_63 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_63,
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__40 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__40,
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_64 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_64,
    form_f1_question_26_4 as form_f1_question_26_4,
    form_f1_question_27_4 as form_f1_question_27_4,
    form_f1_question_28_4 as form_f1_question_28_4,
    form_f1_question_29_4 as form_f1_question_29_4,
    form_f1_question_30_4 as form_f1_question_30_4,
    CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_sf_pcl_5_s4') }}