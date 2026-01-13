{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_spi_subform_1
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
    spi_subform_1_is_medium_risk as spi_subform_1_is_medium_risk,
    spi_subform_1_were_any_of_these_in_the_past_three_months as spi_subform_1_were_any_of_these_in_the_past_three_months,
    spi_subform_1_in_the_past_month_have_you_been_thinking_about_ho as spi_subform_1_in_the_past_month_have_you_been_thinking_about_ho,
    spi_subform_1_is_high_risk as spi_subform_1_is_high_risk,
    spi_subform_1_in_the_past_month_have_you_had_these_thoughts_and as spi_subform_1_in_the_past_month_have_you_had_these_thoughts_and,
    spi_subform_1 as spi_subform_1,
    spi_subform_1_in_the_past_month_have_you_started_to_work_out_or as spi_subform_1_in_the_past_month_have_you_started_to_work_out_or,
    spi_subform_1_2 as spi_subform_1_2,
    spi_subform_1_3 as spi_subform_1_3,
    spi_subform_1_in_the_past_month_have_you_wished_you_were_dead_o as spi_subform_1_in_the_past_month_have_you_wished_you_were_dead_o,
    spi_subform_1_4 as spi_subform_1_4,
    spi_subform_1_have_you_ever_done_anything_started_to_do_anythin as spi_subform_1_have_you_ever_done_anything_started_to_do_anythin,
    spi_subform_1_5 as spi_subform_1_5,
    spi_subform_1_6 as spi_subform_1_6,
    spi_subform_1_is_low_risk as spi_subform_1_is_low_risk,
    spi_subform_1_in_the_past_month_have_you_actually_had_any_thoug as spi_subform_1_in_the_past_month_have_you_actually_had_any_thoug,
    spi_subform_1_7 as spi_subform_1_7,
    a_place_to_declare_values_that_cannot_be_created_using_fhirp_65 as a_place_to_declare_values_that_cannot_be_created_using_fhirp_65,
    spi_subform_1_taskid_spi_subform_2 as spi_subform_1_taskid_spi_subform_2,
    spi_subform_2_taskid_spi_subform_3 as spi_subform_2_taskid_spi_subform_3,
    spi_subform_2_taskid_spi_subform_4 as spi_subform_2_taskid_spi_subform_4,
    spi_subform_1_taskstatus_spi_subform_2 as spi_subform_1_taskstatus_spi_subform_2,
    spi_subform_2_taskstatus_spi_subform_3 as spi_subform_2_taskstatus_spi_subform_3,
    spi_subform_2_taskstatus_spi_subform_4 as spi_subform_2_taskstatus_spi_subform_4,
    CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_spi_subform_1') }}