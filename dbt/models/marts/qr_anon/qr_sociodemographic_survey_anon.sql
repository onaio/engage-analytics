{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_sociodemographic_survey
-- Automatically generated based on questionnaire_metadata.csv

select
    questionnaire_id as questionnaire_id,
    subject_patient_id as subject_patient_id,
    encounter_id as encounter_id,
    author_practitioner_id as author_practitioner_id,
    practitioner_location_id as practitioner_location_id,
    practitioner_organization_id as practitioner_organization_id,
    practitioner_id as practitioner_id,
    practitioner_careteam_id as practitioner_careteam_id,
    application_version as application_version,
    qr_id as qr_id,
    sociodemographic_survey_if_you_were_not_born_in_the_us_how_long as sociodemographic_survey_if_you_were_not_born_in_the_us_how_long,
    sociodemographic_survey_please_specify as sociodemographic_survey_please_specify,
    sociodemographic_survey_please_specify_2 as sociodemographic_survey_please_specify_2,
    sociodemographic_survey_where_were_you_born as sociodemographic_survey_where_were_you_born,
    demographic_question as demographic_question,
    demographic_question_2 as demographic_question_2,
    sociodemographic_survey_please_enter_the_number_of_years as sociodemographic_survey_please_enter_the_number_of_years,
    sociodemographic_survey_we_would_like_to_know_about_what_you_do as sociodemographic_survey_we_would_like_to_know_about_what_you_do,
    demographic_question_3 as demographic_question_3,
    sociodemographic_survey_please_enter_the_number_of_months as sociodemographic_survey_please_enter_the_number_of_months,
    sociodemographic_survey_what_is_your_race_and_or_ethnicity as sociodemographic_survey_what_is_your_race_and_or_ethnicity,
    demographic_question_4 as demographic_question_4,
    demographic_question_5 as demographic_question_5,
    sociodemographic_survey_what_is_your_marital_status as sociodemographic_survey_what_is_your_marital_status,
    demographic_question_6 as demographic_question_6,
    sociodemographic_survey_what_was_the_highest_grade_or_level_of_ as sociodemographic_survey_what_was_the_highest_grade_or_level_of_,
    sociodemographic_survey_please_specify_3 as sociodemographic_survey_please_specify_3,
    'REDACTED' as patient_age_6,
    'REDACTED' as patient_biological_sex_3,
    'REDACTED' as patient_date_of_birth_6,
    'REDACTED' as patient_gender_identity_3,
    'REDACTED' as patient_how_you_think_of_yourself_3,
    'REDACTED' as patient_name_6,
    'REDACTED' as patient_pronouns_3,
    task_id_socio_pdf_2 as task_id_socio_pdf_2,
    CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_sociodemographic_survey') }}