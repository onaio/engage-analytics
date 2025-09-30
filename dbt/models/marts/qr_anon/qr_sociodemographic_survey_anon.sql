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
    you_not_born as you_not_born,
    please_specify as please_specify,
    please_specify_3 as please_specify_3,
    you_born as you_born,
    demographic_question as demographic_question,
    demographic_question_6 as demographic_question_6,
    please_enter_number as please_enter_number,
    like_know_you as like_know_you,
    demographic_question_9 as demographic_question_9,
    please_enter_number_10 as please_enter_number_10,
    your_race_ethnicity as your_race_ethnicity,
    demographic_question_12 as demographic_question_12,
    demographic_question_13 as demographic_question_13,
    your_marital_status as your_marital_status,
    demographic_question_15 as demographic_question_15,
    highest_grade_level as highest_grade_level,
    please_specify_17 as please_specify_17,
    'REDACTED' as patient_age,
    'REDACTED' as patient_biological_sex,
    'REDACTED' as patient_dob,
    'REDACTED' as patient_gender_identity,
    'REDACTED' as patient_how_you_think_of_yourself,
    'REDACTED' as patient_name,
    'REDACTED' as patient_pronouns,
    task_socio_pdf as task_socio_pdf,
    CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_sociodemographic_survey') }}