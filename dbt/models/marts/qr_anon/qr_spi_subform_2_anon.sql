{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_spi_subform_2 with PII fields masked based on questionnaire_metadata.anon flag
-- Questionnaire: SPI Subform 2 (Questionnaire/104453)
-- PII fields masked: 2 fields

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
        spi_encounter_id_of_spi_sub_2,
        spi_did_this_client_also_screen_positive_for_probable_,
        spi_did_you_discuss_this_clients_severe_mental_health_,
        'REDACTED' as spi_what_is_the_recommended_plan_to_address_the_probab,
        'REDACTED' as spi_what_is_the_recommended_plan_to_address_the_probab_5,
        spi_taskid_spi_subform_3,
        spi_d02eb882411e4bc0af9d08a77fcc3f50,
        spi_taskstatus_spi_subform_3,
        spi_taskid_spi_subform_4,
        spi_taskstatus_spi_subform_4,
        spi_53b30f1274eb474c941c5f0e81b7eb9f,
        spi_did_you_discuss_this_clients_suicide_risk_screenin,
        spi_this_client_scored_in_the_moderate_to_high_range_o,
        spi_783e2646c8ea466dbba885c891a14435,
        CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_spi_subform_2') }}