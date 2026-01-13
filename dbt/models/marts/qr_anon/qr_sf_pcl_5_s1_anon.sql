{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_sf_pcl_5_s1
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
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_43 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_43,
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_44 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_44,
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_9 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_9,
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__10 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__10,
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__11 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__11,
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__12 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__12,
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__13 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__13,
    pcl_5_item as pcl_5_item,
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__14 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__14,
    add_family_member_registration_calculated_age_8 as add_family_member_registration_calculated_age_8,
    pcl_5_item_2 as pcl_5_item_2,
    add_family_member_registration_calculated_month_8 as add_family_member_registration_calculated_month_8,
    add_family_member_registration_calculated_year_8 as add_family_member_registration_calculated_year_8,
    add_family_member_registration_date_of_birth_8 as add_family_member_registration_date_of_birth_8,
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_45 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_45,
    pcl_5_item_3 as pcl_5_item_3,
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__15 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__15,
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_46 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_46,
    short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__16 as short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__16,
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_47 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_47,
    form_f1_question_26 as form_f1_question_26,
    form_f1_question_27 as form_f1_question_27,
    form_f1_question_28 as form_f1_question_28,
    form_f1_question_29 as form_f1_question_29,
    form_f1_question_30 as form_f1_question_30,
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_48 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_48,
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_49 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_49,
    CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_sf_pcl_5_s1') }}