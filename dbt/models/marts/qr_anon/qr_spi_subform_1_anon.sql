{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_spi_subform_1 with PII fields masked based on questionnaire_metadata.anon flag
-- Questionnaire: SPI Subform 1 (Questionnaire/104455)
-- PII fields masked: 1 fields

select 
    MD5(COALESCE(qr_id, '')::text) as qr_id_hash,
    questionnaire_id,
    MD5(COALESCE(subject_patient_id, '')::text) as subject_patient_id_hash,
    encounter_id,
    author_practitioner_id,
    practitioner_location_id,
    practitioner_organization_id,
    practitioner_id,
    practitioner_careteam_id,
    application_version,
        spi_encounter_id_of_spi_sub_1,
        spi_is_low_risk,
        spi_is_medium_risk,
        spi_is_high_risk,
        spi_were_any_of_these_in_the_past_three_months,
        spi_in_the_past_month_have_you_been_thinking_about_how,
        'REDACTED' as spi_in_the_past_month_have_you_had_these_thoughts_and_,
        spi_in_the_past_month_have_you_started_to_work_out_or_,
        spi_in_the_past_month_have_you_wished_you_were_dead_or,
        spi_have_you_ever_done_anything_started_to_do_anything,
        spi_in_the_past_month_have_you_actually_had_any_though,
        spi_taskid_spi_subform_2,
        spi_43898926e9d5400ea09562a8c7f8dd03,
        spi_644c00f28b07491192d0f05f5e5cc837,
        spi_71dbaac9867f4d7fb6d7e1f59402bbf9,
        spi_7fc39fba84db490daf4e8f772d91ffda,
        spi_a6601f970a3449d0b181611d6705ca42,
        spi_c7d21a570fc64e3ab411beceb1fb2423,
        spi_ecabd308c45a41f78754b1f63a101019,
        spi_taskstatus_spi_subform_2,
        "task-id-spi-subform-3",
        "task-id-spi-subform-4",
        "task-status-spi-subform-3",
        "task-status-spi-subform-4",
        CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_spi_subform_1') }}