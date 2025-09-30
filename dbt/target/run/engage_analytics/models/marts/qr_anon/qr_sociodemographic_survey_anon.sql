
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_sociodemographic_survey_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_sociodemographic_survey
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_sociodemographic_survey'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_sociodemographic_survey"
)

select
    

    -- Hash patient and QR IDs for privacy
    MD5(qr_id::text) as qr_id_hash,
    questionnaire_id,
    MD5(COALESCE(subject_patient_id, '')::text) as subject_patient_id_hash,
    encounter_id,
    author_practitioner_id,
    practitioner_location_id,
    practitioner_organization_id,
    practitioner_id,
    practitioner_careteam_id,
    application_version,

    -- Process all other columns dynamically
    
        
    
        
    
        
    
        
    
        
    
        
    
        
    
        
    
        
    
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sociodemographic_survey_if_you_were_not_born_in_the_us_how_long' OR linkid = 'sociodemographic_survey_if_you_were_not_born_in_the_us_how_long' OR short_name = 'sociodemographic_survey_if_you_were_not_born_in_the_us_how_long')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sociodemographic_survey_if_you_were_not_born_in_the_us_how_long"
            END as "sociodemographic_survey_if_you_were_not_born_in_the_us_how_long",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sociodemographic_survey_please_specify' OR linkid = 'sociodemographic_survey_please_specify' OR short_name = 'sociodemographic_survey_please_specify')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sociodemographic_survey_please_specify"
            END as "sociodemographic_survey_please_specify",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sociodemographic_survey_please_specify_2' OR linkid = 'sociodemographic_survey_please_specify_2' OR short_name = 'sociodemographic_survey_please_specify_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sociodemographic_survey_please_specify_2"
            END as "sociodemographic_survey_please_specify_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sociodemographic_survey_where_were_you_born' OR linkid = 'sociodemographic_survey_where_were_you_born' OR short_name = 'sociodemographic_survey_where_were_you_born')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sociodemographic_survey_where_were_you_born"
            END as "sociodemographic_survey_where_were_you_born",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'demographic_question' OR linkid = 'demographic_question' OR short_name = 'demographic_question')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "demographic_question"
            END as "demographic_question",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'demographic_question_2' OR linkid = 'demographic_question_2' OR short_name = 'demographic_question_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "demographic_question_2"
            END as "demographic_question_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sociodemographic_survey_please_enter_the_number_of_years' OR linkid = 'sociodemographic_survey_please_enter_the_number_of_years' OR short_name = 'sociodemographic_survey_please_enter_the_number_of_years')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sociodemographic_survey_please_enter_the_number_of_years"
            END as "sociodemographic_survey_please_enter_the_number_of_years",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sociodemographic_survey_we_would_like_to_know_about_what_you_do' OR linkid = 'sociodemographic_survey_we_would_like_to_know_about_what_you_do' OR short_name = 'sociodemographic_survey_we_would_like_to_know_about_what_you_do')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sociodemographic_survey_we_would_like_to_know_about_what_you_do"
            END as "sociodemographic_survey_we_would_like_to_know_about_what_you_do",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'demographic_question_3' OR linkid = 'demographic_question_3' OR short_name = 'demographic_question_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "demographic_question_3"
            END as "demographic_question_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sociodemographic_survey_please_enter_the_number_of_months' OR linkid = 'sociodemographic_survey_please_enter_the_number_of_months' OR short_name = 'sociodemographic_survey_please_enter_the_number_of_months')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sociodemographic_survey_please_enter_the_number_of_months"
            END as "sociodemographic_survey_please_enter_the_number_of_months",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sociodemographic_survey_what_is_your_race_and_or_ethnicity' OR linkid = 'sociodemographic_survey_what_is_your_race_and_or_ethnicity' OR short_name = 'sociodemographic_survey_what_is_your_race_and_or_ethnicity')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sociodemographic_survey_what_is_your_race_and_or_ethnicity"
            END as "sociodemographic_survey_what_is_your_race_and_or_ethnicity",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'demographic_question_4' OR linkid = 'demographic_question_4' OR short_name = 'demographic_question_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "demographic_question_4"
            END as "demographic_question_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'demographic_question_5' OR linkid = 'demographic_question_5' OR short_name = 'demographic_question_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "demographic_question_5"
            END as "demographic_question_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sociodemographic_survey_what_is_your_marital_status' OR linkid = 'sociodemographic_survey_what_is_your_marital_status' OR short_name = 'sociodemographic_survey_what_is_your_marital_status')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sociodemographic_survey_what_is_your_marital_status"
            END as "sociodemographic_survey_what_is_your_marital_status",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'demographic_question_6' OR linkid = 'demographic_question_6' OR short_name = 'demographic_question_6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "demographic_question_6"
            END as "demographic_question_6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sociodemographic_survey_what_was_the_highest_grade_or_level_of_' OR linkid = 'sociodemographic_survey_what_was_the_highest_grade_or_level_of_' OR short_name = 'sociodemographic_survey_what_was_the_highest_grade_or_level_of_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sociodemographic_survey_what_was_the_highest_grade_or_level_of_"
            END as "sociodemographic_survey_what_was_the_highest_grade_or_level_of_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sociodemographic_survey_please_specify_3' OR linkid = 'sociodemographic_survey_please_specify_3' OR short_name = 'sociodemographic_survey_please_specify_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sociodemographic_survey_please_specify_3"
            END as "sociodemographic_survey_please_specify_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'patient-age' OR linkid = 'patient-age' OR short_name = 'patient-age')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "patient-age"
            END as "patient-age",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'patient-biological-sex' OR linkid = 'patient-biological-sex' OR short_name = 'patient-biological-sex')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "patient-biological-sex"
            END as "patient-biological-sex",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'patient-dob' OR linkid = 'patient-dob' OR short_name = 'patient-dob')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "patient-dob"
            END as "patient-dob",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'patient-gender-identity' OR linkid = 'patient-gender-identity' OR short_name = 'patient-gender-identity')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "patient-gender-identity"
            END as "patient-gender-identity",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'patient-how-you-think-of-yourself' OR linkid = 'patient-how-you-think-of-yourself' OR short_name = 'patient-how-you-think-of-yourself')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "patient-how-you-think-of-yourself"
            END as "patient-how-you-think-of-yourself",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'patient-name' OR linkid = 'patient-name' OR short_name = 'patient-name')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "patient-name"
            END as "patient-name",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'patient-pronouns' OR linkid = 'patient-pronouns' OR short_name = 'patient-pronouns')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "patient-pronouns"
            END as "patient-pronouns",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'task_id_socio_pdf_2' OR linkid = 'task_id_socio_pdf_2' OR short_name = 'task_id_socio_pdf_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "task_id_socio_pdf_2"
            END as "task_id_socio_pdf_2",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data


  );