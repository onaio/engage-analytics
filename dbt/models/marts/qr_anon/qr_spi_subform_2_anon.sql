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
    client_also_screen as client_also_screen,
    you_discuss_client as you_discuss_client,
    client_scored_moderate as client_scored_moderate,
    recommended_plan_address as recommended_plan_address,
    spi_subform as spi_subform,
    spi_subform_6 as spi_subform_6,
    spi_subform_7 as spi_subform_7,
    recommended_plan_address_8 as recommended_plan_address_8,
    place_declare_values as place_declare_values,
    you_discuss_client_10 as you_discuss_client_10,
    taskid_spi_subform as taskid_spi_subform,
    taskid_spi_subform_12 as taskid_spi_subform_12,
    taskstatus_spi_subform as taskstatus_spi_subform,
    taskstatus_spi_subform_14 as taskstatus_spi_subform_14,
    CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_spi_subform_2') }}