{{
    config(
        materialized='view'
    )
}}

-- Anonymized registration info with PII fields dynamically masked based on questionnaire_metadata.anon flag
-- Uses the anon column to determine which fields should be masked

with metadata_pii as (
    select 
        question_alias,
        question_text
    from {{ ref('questionnaire_metadata') }}
    where questionnaire_id = 'Questionnaire/221'
    and anon = 'TRUE'
),

registration_data as (
    select * from {{ ref('qr_registration_info') }}
)

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
    
    -- Mask PII fields based on metadata
    -- Unique ID
    CASE WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'registration_unique_id')
        THEN 'REDACTED'
        ELSE registration_unique_id
    END as registration_unique_id_masked,
    
    -- Names
    CASE WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'registration_first_name')
        THEN 'REDACTED'
        ELSE registration_first_name
    END as registration_first_name_masked,
    
    CASE WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'registration_middle_name')
        THEN 'REDACTED'
        ELSE registration_middle_name
    END as registration_middle_name_masked,
    
    CASE WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'registration_surname')
        THEN 'REDACTED'
        ELSE registration_surname
    END as registration_surname_masked,
    
    -- Date of birth
    CASE WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'registration_date_of_birth_dob')
        THEN 'REDACTED'
        ELSE registration_date_of_birth_dob
    END as registration_date_of_birth_dob_masked,
    
    -- Age fields (keep these as they're derived/less sensitive)
    registration_age,
    registration_age_6,
    registration_calculated_age,
    
    -- Contact information (if they exist in the data)
    CASE 
        WHEN "email-address" IS NOT NULL THEN 'REDACTED'
        ELSE NULL
    END as "email-address_masked",
    
    CASE 
        WHEN "phone-number" IS NOT NULL AND LENGTH("phone-number") >= 4
        THEN 'XXX-XXX-' || RIGHT("phone-number", 4)
        WHEN "phone-number" IS NOT NULL
        THEN 'XXX-XXX-' || "phone-number"
        ELSE 'NO PHONE'
    END as "phone-number_masked",
    
    CASE 
        WHEN "physical-address" IS NOT NULL THEN 'REDACTED'
        ELSE NULL
    END as "physical-address_masked",
    
    CASE 
        WHEN "zip-code" IS NOT NULL 
        THEN LEFT("zip-code", 3) || 'XX'
        ELSE NULL
    END as "zip-code_masked",
    
    -- Medicaid information
    "do-you-have-medicaid",
    CASE 
        WHEN "medicaid-number" IS NOT NULL THEN 'REDACTED'
        ELSE NULL
    END as "medicaid-number_masked",
    
    -- Non-PII demographic fields (keep as-is)
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

from registration_data