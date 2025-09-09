{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_confidential_s1 with PII fields masked based on questionnaire_metadata.anon flag
-- Questionnaire: Discuss Confidentiality (Questionnaire/23451)
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
        'REDACTED' as date_of_birth,
        age,
        birth_month,
        age_years,
        encounter_reference,
        CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_confidential_s1') }}