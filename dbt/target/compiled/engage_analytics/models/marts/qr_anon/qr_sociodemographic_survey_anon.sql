

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
    zip_code_you as zip_code_you,
    your_biological_sex as your_biological_sex,
    please_enter_number as please_enter_number,
    like_know_you as like_know_you,
    demographic_question as demographic_question,
    please_enter_number_10 as please_enter_number_10,
    your_race_ethnicity as your_race_ethnicity,
    following_best_represents as following_best_represents,
    client_first_learn as client_first_learn,
    your_marital_status as your_marital_status,
    term_best_expresses as term_best_expresses,
    highest_grade_level as highest_grade_level,
    please_specify_territory as please_specify_territory,
    'REDACTED' as patient_age,
    'REDACTED' as patient_biological_sex,
    'REDACTED' as patient_dob,
    'REDACTED' as patient_gender_identity,
    'REDACTED' as patient_how_you_think_of_yourself,
    'REDACTED' as patient_name,
    'REDACTED' as patient_pronouns,
    task_socio_pdf as task_socio_pdf,
    CURRENT_TIMESTAMP as anonymized_at

from "airbyte"."engage_analytics_engage_analytics_mart"."qr_sociodemographic_survey"