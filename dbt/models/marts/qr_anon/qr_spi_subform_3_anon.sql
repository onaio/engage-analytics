{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_spi_subform_3 with PII fields masked based on questionnaire_metadata.anon flag
-- Questionnaire: Spi Subform 3 (Questionnaire/104454)
-- PII fields masked: 0 fields

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
        spi_encounter_id_of_spi_sub_3,
        CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_spi_subform_3') }}