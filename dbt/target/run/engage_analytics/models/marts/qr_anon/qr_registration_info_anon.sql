
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_registration_info_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_registration_info with PII fields masked based on questionnaire_metadata.anon flag
-- Questionnaire: Add Family Member Registration (Questionnaire/221)
-- PII fields masked: 11 fields

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
        registration_36d39dbe00e24e29f0793ec8f0119aff,
        'REDACTED' as registration_middle_name,
        registration_which_of_the_following_best_represents_how_you_thi,
        registration_age,
        'REDACTED' as registration_surname,
        registration_age_6,
        registration_calculated_age,
        'REDACTED' as registration_unique_id,
        registration_has_this_person_received_engage_services_in_the_pa,
        registration_calculated_month,
        registration_calculated_year,
        'REDACTED' as registration_date_of_birth_dob,
        'REDACTED' as registration_first_name,
        registration_what_was_your_biological_sex_assigned_at_birth,
        "choice-gender-identity",
        "choice-pronouns",
        'REDACTED' as "do-you-have-medicaid",
        'REDACTED' as "email-address",
        'REDACTED' as "medicaid-number",
        "other-best-represents-how-you-think-of-yourself",
        "other-biological-sex-at-birth",
        "other-gender-identity",
        "other-pronouns",
        "other-where-did-you-first-learn-about-engage",
        CASE 
        WHEN "phone-number" IS NOT NULL AND LENGTH("phone-number"::text) >= 4
        THEN 'XXX-XXX-' || RIGHT("phone-number"::text, 4)
        WHEN "phone-number" IS NOT NULL
        THEN 'XXX-XXX-' || "phone-number"::text
        ELSE 'NO PHONE'
    END as "phone-number",
        'REDACTED' as "physical-address",
        "where-did-you-first-learn-about-engage",
        CASE 
        WHEN "zip-code" IS NOT NULL 
        THEN LEFT("zip-code"::text, 3) || 'XX'
        ELSE NULL
    END as "zip-code",
        CURRENT_TIMESTAMP as anonymized_at

from "airbyte"."engage_analytics_engage_analytics_mart"."qr_registration_info"
  );