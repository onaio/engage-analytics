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
    medium_risk as medium_risk,
    any_three as any_three,
    you_thinking_you as you_thinking_you,
    high_risk as high_risk,
    you_thoughts_some as you_thoughts_some,
    spi_subform as spi_subform,
    you_started_work as you_started_work,
    spi_subform_8 as spi_subform_8,
    spi_subform_9 as spi_subform_9,
    you_wished_you as you_wished_you,
    spi_subform_11 as spi_subform_11,
    you_ever_done as you_ever_done,
    spi_subform_13 as spi_subform_13,
    spi_subform_14 as spi_subform_14,
    low_risk as low_risk,
    you_actually_any as you_actually_any,
    spi_subform_17 as spi_subform_17,
    place_declare_values as place_declare_values,
    taskid_spi_subform as taskid_spi_subform,
    taskid_spi_subform_20 as taskid_spi_subform_20,
    taskid_spi_subform_21 as taskid_spi_subform_21,
    taskstatus_spi_subform as taskstatus_spi_subform,
    taskstatus_spi_subform_23 as taskstatus_spi_subform_23,
    taskstatus_spi_subform_24 as taskstatus_spi_subform_24,
    CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_spi_subform_1') }}