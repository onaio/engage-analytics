{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_spi_subform_2
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
    spi_subform_2_did_this_client_also_screen_positive_for_probable as spi_subform_2_did_this_client_also_screen_positive_for_probable,
    spi_subform_2_did_you_discuss_this_client_s_severe_mental_healt as spi_subform_2_did_you_discuss_this_client_s_severe_mental_healt,
    spi_subform_2_this_client_scored_in_the_moderate_to_high_range_ as spi_subform_2_this_client_scored_in_the_moderate_to_high_range_,
    spi_subform_2_what_is_the_recommended_plan_to_address_the_proba as spi_subform_2_what_is_the_recommended_plan_to_address_the_proba,
    spi_subform_2 as spi_subform_2,
    spi_subform_2_2 as spi_subform_2_2,
    spi_subform_2_3 as spi_subform_2_3,
    spi_subform_2_what_is_the_recommended_plan_to_address_the_pro_2 as spi_subform_2_what_is_the_recommended_plan_to_address_the_pro_2,
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_66 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_66,
    spi_subform_2_did_you_discuss_this_client_s_suicide_risk_screen as spi_subform_2_did_you_discuss_this_client_s_suicide_risk_screen,
    spi_subform_2_taskid_spi_subform_3_2 as spi_subform_2_taskid_spi_subform_3_2,
    spi_subform_2_taskid_spi_subform_4_2 as spi_subform_2_taskid_spi_subform_4_2,
    spi_subform_2_taskstatus_spi_subform_3_2 as spi_subform_2_taskstatus_spi_subform_3_2,
    spi_subform_2_taskstatus_spi_subform_4_2 as spi_subform_2_taskstatus_spi_subform_4_2,
    CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_spi_subform_2') }}