{{
    config(
        materialized='view'
    )
}}

-- Anonymized patient view with masked PII fields for data sharing
-- Masks: Unique ID, First/Middle/Last names, DOB (shows age instead), Phone, Email, Address, Zip, Medicaid number

select
    -- Use hashed ID for referential integrity while maintaining privacy
    MD5(id::text) as patient_id_hash,

    -- Demographic information (non-PII)
    gender,
    age_years,  -- Age instead of birth_date for privacy
    deceased,
    active,
    registration_date,

    -- Masked name fields
    'REDACTED' as name_family_masked,
    'REDACTED' as name_given_masked,

    -- Masked contact information
    CASE
        WHEN phone_number IS NOT NULL AND LENGTH(phone_number) >= 4
        THEN 'XXX-XXX-' || RIGHT(phone_number, 4)
        ELSE 'NO PHONE'
    END as phone_number_masked,

    -- Location and organization (reference IDs only, no PII)
    location_id,
    practitioner_id,
    practitioner_organization_id,
    practitioner_careteam_id,

    -- Metadata
    _airbyte_emitted_at,
    CURRENT_TIMESTAMP as anonymized_at

from {{ ref('patient') }}