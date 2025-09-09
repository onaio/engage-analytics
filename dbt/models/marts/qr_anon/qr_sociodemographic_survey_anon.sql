{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_sociodemographic_survey
-- Based on actual data columns, with patient- fields marked as PII

select 
    -- System fields
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
    
    -- Demographic fields (keep most as-is except known PII)
    demographic_if_you_were_not_born_in_the_us_how_long_have_you_l,
    demographic_please_specify,
    demographic_please_specify_3,
    demographic_where_were_you_born,
    
    -- Zip code - mask last 2 digits
    CASE 
        WHEN demographic_what_is_the_zip_code_of_where_you_currently_live IS NOT NULL 
        THEN LEFT(demographic_what_is_the_zip_code_of_where_you_currently_live, 3) || 'XX'
        ELSE NULL
    END as demographic_what_is_the_zip_code_of_where_you_currently_live,
    
    demographic_what_was_your_biological_sex_at_birth,
    demographic_please_enter_the_number_of_years,
    demographic_we_would_like_to_know_about_what_you_do_are_you,
    demographic_please_enter_the_number_of_months,
    demographic_what_is_your_race_andor_ethnicity,
    demographic_which_of_the_following_best_represents_how_you_thi,
    demographic_where_did_this_client_first_learn_about_engage,
    demographic_what_is_your_marital_status,
    demographic_what_term_best_expresses_how_you_describe_your_gen,
    demographic_what_was_the_highest_grade_or_level_of_school_that,
    demographic_please_specify_the_us_territorycommonwealth_eg_pue,
    some_of_the_time,
    
    -- Patient fields - ALL should be masked as PII
    'REDACTED' as "patient-age",
    'REDACTED' as "patient-biological-sex",
    'REDACTED' as "patient-dob",
    'REDACTED' as "patient-gender-identity",
    'REDACTED' as "patient-how-you-think-of-yourself",
    'REDACTED' as "patient-name",
    'REDACTED' as "patient-pronouns",
    
    -- System generated field
    "task-id-socio-pdf",
    
    CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_sociodemographic_survey') }}