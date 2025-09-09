

-- Anonymized patient view with masked PII fields for data sharing
-- Masks: Unique ID, First/Middle/Last names, DOB (shows age instead), Phone, Email, Address, Zip, Medicaid number
-- Pulls phone numbers from registration data when available

with patient_with_registration as (
    select 
        p.*,
        r.phone_number as registration_phone
    from "airbyte"."engage_analytics_engage_analytics_mart"."patient" p
    left join "airbyte"."engage_analytics_engage_analytics_mart"."qr_registration_info" r on p.id = r.subject_patient_id
)

select 
    -- Use hashed ID for referential integrity while maintaining privacy
    MD5(p.id::text) as patient_id_hash,
    
    -- Demographic information (non-PII)
    p.gender,
    p.age_years,  -- Age instead of birth_date for privacy
    p.deceased,
    p.active,
    p.registration_date,
    
    -- Masked name fields
    'REDACTED' as name_family_masked,
    'REDACTED' as name_given_masked,
    
    -- Masked contact information (use registration phone if patient phone is null)
    CASE 
        WHEN COALESCE(p.phone_number, p.registration_phone) IS NOT NULL 
            AND LENGTH(COALESCE(p.phone_number, p.registration_phone)) >= 4
        THEN 'XXX-XXX-' || RIGHT(COALESCE(p.phone_number, p.registration_phone), 4)
        ELSE 'NO PHONE'
    END as phone_number_masked,
    
    -- Location and organization (reference IDs only, no PII)
    p.location_id,
    p.practitioner_id,
    p.practitioner_organization_id,
    p.practitioner_careteam_id,
    
    -- Metadata
    p._airbyte_emitted_at,
    CURRENT_TIMESTAMP as anonymized_at

from patient_with_registration p