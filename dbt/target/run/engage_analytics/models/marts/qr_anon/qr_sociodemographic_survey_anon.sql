
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_sociodemographic_survey_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_sociodemographic_survey with PII fields masked based on questionnaire_metadata.anon flag
-- Auto-generated to mask fields marked as anon=TRUE in questionnaire_metadata

with metadata_pii as (
    select distinct
        question_alias
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where questionnaire_id in ('Questionnaire/1613531')
    and anon = 'TRUE'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_sociodemographic_survey"
)

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
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'demographic_if_you_were_not_born_in_the_us_how_long_have_you_l')
        THEN 'REDACTED'
        ELSE demographic_if_you_were_not_born_in_the_us_how_long_have_you_l::text
    END as demographic_if_you_were_not_born_in_the_us_how_long_have_you_l,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'demographic_please_specify')
        THEN 'REDACTED'
        ELSE demographic_please_specify::text
    END as demographic_please_specify,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'demographic_please_specify_3')
        THEN 'REDACTED'
        ELSE demographic_please_specify_3::text
    END as demographic_please_specify_3,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'demographic_where_were_you_born')
        THEN 'REDACTED'
        ELSE demographic_where_were_you_born::text
    END as demographic_where_were_you_born,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'demographic_what_is_the_zip_code_of_where_you_currently_live')
        THEN 
            CASE 
                WHEN demographic_what_is_the_zip_code_of_where_you_currently_live IS NOT NULL 
                THEN LEFT(demographic_what_is_the_zip_code_of_where_you_currently_live::text, 3) || 'XX'
                ELSE NULL
            END
        ELSE demographic_what_is_the_zip_code_of_where_you_currently_live::text
    END as demographic_what_is_the_zip_code_of_where_you_currently_live,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'demographic_what_was_your_biological_sex_at_birth')
        THEN 'REDACTED'
        ELSE demographic_what_was_your_biological_sex_at_birth::text
    END as demographic_what_was_your_biological_sex_at_birth,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'demographic_please_enter_the_number_of_years')
        THEN 'REDACTED'
        ELSE demographic_please_enter_the_number_of_years::text
    END as demographic_please_enter_the_number_of_years,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'demographic_we_would_like_to_know_about_what_you_do_are_you')
        THEN 'REDACTED'
        ELSE demographic_we_would_like_to_know_about_what_you_do_are_you::text
    END as demographic_we_would_like_to_know_about_what_you_do_are_you,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'demographic_please_enter_the_number_of_months')
        THEN 'REDACTED'
        ELSE demographic_please_enter_the_number_of_months::text
    END as demographic_please_enter_the_number_of_months,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'demographic_what_is_your_race_andor_ethnicity')
        THEN 'REDACTED'
        ELSE demographic_what_is_your_race_andor_ethnicity::text
    END as demographic_what_is_your_race_andor_ethnicity,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'demographic_which_of_the_following_best_represents_how_you_thi')
        THEN 'REDACTED'
        ELSE demographic_which_of_the_following_best_represents_how_you_thi::text
    END as demographic_which_of_the_following_best_represents_how_you_thi,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'demographic_where_did_this_client_first_learn_about_engage')
        THEN 'REDACTED'
        ELSE demographic_where_did_this_client_first_learn_about_engage::text
    END as demographic_where_did_this_client_first_learn_about_engage,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'demographic_what_is_your_marital_status')
        THEN 'REDACTED'
        ELSE demographic_what_is_your_marital_status::text
    END as demographic_what_is_your_marital_status,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'demographic_what_term_best_expresses_how_you_describe_your_gen')
        THEN 'REDACTED'
        ELSE demographic_what_term_best_expresses_how_you_describe_your_gen::text
    END as demographic_what_term_best_expresses_how_you_describe_your_gen,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'demographic_what_was_the_highest_grade_or_level_of_school_that')
        THEN 'REDACTED'
        ELSE demographic_what_was_the_highest_grade_or_level_of_school_that::text
    END as demographic_what_was_the_highest_grade_or_level_of_school_that,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'demographic_please_specify_the_us_territorycommonwealth_eg_pue')
        THEN 'REDACTED'
        ELSE demographic_please_specify_the_us_territorycommonwealth_eg_pue::text
    END as demographic_please_specify_the_us_territorycommonwealth_eg_pue,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'some_of_the_time')
        THEN 'REDACTED'
        ELSE some_of_the_time::text
    END as some_of_the_time,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'patient-age')
        THEN 'REDACTED'
        ELSE "patient-age"::text
    END as "patient-age",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'patient-biological-sex')
        THEN 'REDACTED'
        ELSE "patient-biological-sex"::text
    END as "patient-biological-sex",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'patient-dob')
        THEN 'REDACTED'
        ELSE "patient-dob"::text
    END as "patient-dob",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'patient-gender-identity')
        THEN 'REDACTED'
        ELSE "patient-gender-identity"::text
    END as "patient-gender-identity",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'patient-how-you-think-of-yourself')
        THEN 'REDACTED'
        ELSE "patient-how-you-think-of-yourself"::text
    END as "patient-how-you-think-of-yourself",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'patient-name')
        THEN 'REDACTED'
        ELSE "patient-name"::text
    END as "patient-name",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'patient-pronouns')
        THEN 'REDACTED'
        ELSE "patient-pronouns"::text
    END as "patient-pronouns",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-id-socio-pdf')
        THEN 'REDACTED'
        ELSE "task-id-socio-pdf"::text
    END as "task-id-socio-pdf",
        CURRENT_TIMESTAMP as anonymized_at

from source_data
  );