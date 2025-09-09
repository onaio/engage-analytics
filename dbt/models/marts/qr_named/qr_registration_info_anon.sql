{{
    config(
        materialized='view'
    )
}}

-- Anonymized registration info with masked PII fields
-- Masks: Unique ID, Names, DOB, Phone, Email, Physical address, Zip code, Medicaid number

select 
    -- Non-PII identifiers (hashed for referential integrity)
    MD5(qr_id::text) as qr_id_hash,
    questionnaire_id,
    MD5(subject_patient_id::text) as subject_patient_id_hash,
    encounter_id,
    author_practitioner_id,
    
    -- Location and organization references (non-PII)
    practitioner_location_id,
    practitioner_organization_id,
    practitioner_id,
    practitioner_careteam_id,
    
    -- Application metadata
    application_version,
    
    -- Masked PII fields
    'REDACTED' as registration_unique_id_masked,
    'REDACTED' as registration_first_name_masked,
    'REDACTED' as registration_middle_name_masked,
    'REDACTED' as registration_surname_masked,
    
    -- Age fields (keep age, mask DOB)
    registration_age,
    registration_age_6,
    registration_calculated_age,
    'REDACTED' as registration_date_of_birth_dob_masked,
    
    -- Masked contact information
    'REDACTED' as "email-address_masked",
    CASE 
        WHEN "phone-number" IS NOT NULL AND LENGTH("phone-number") >= 4
        THEN 'XXX-XXX-' || RIGHT("phone-number", 4)
        WHEN "phone-number" IS NOT NULL
        THEN 'XXX-XXX-' || "phone-number"
        ELSE 'NO PHONE'
    END as "phone-number_masked",
    'REDACTED' as "physical-address_masked",
    CASE 
        WHEN "zip-code" IS NOT NULL 
        THEN LEFT("zip-code", 3) || 'XX'
        ELSE NULL
    END as "zip-code_masked",
    
    -- Masked Medicaid information
    CASE 
        WHEN "do-you-have-medicaid" = 'Yes' 
        THEN 'Yes'
        ELSE "do-you-have-medicaid"
    END as "do-you-have-medicaid",
    'REDACTED' as "medicaid-number_masked",
    
    -- Non-PII demographic fields
    registration_which_of_the_following_best_represents_how_you_thi,
    registration_what_was_your_biological_sex_assigned_at_birth,
    "choice-gender-identity",
    "choice-pronouns",
    "other-best-represents-how-you-think-of-yourself",
    
    -- Service history (non-PII)
    registration_has_this_person_received_engage_services_in_the_pa,
    
    -- Date components (non-specific)
    registration_calculated_month,
    registration_calculated_year,
    
    -- Metadata
    CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_registration_info') }}