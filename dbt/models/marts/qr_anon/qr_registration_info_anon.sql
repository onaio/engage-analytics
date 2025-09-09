{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_registration_info
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
    add_family_member as add_family_member,
    add_family_member_2 as add_family_member_2,
    add_family_member_3 as add_family_member_3,
    add_family_member_4 as add_family_member_4,
    add_family_member_5 as add_family_member_5,
    q_age as q_age,
    add_family_member_7 as add_family_member_7,
    add_family_member_8 as add_family_member_8,
    add_family_member_9 as add_family_member_9,
    add_family_member_10 as add_family_member_10,
    add_family_member_11 as add_family_member_11,
    add_family_member_12 as add_family_member_12,
    choice_gender_identity as choice_gender_identity,
    choice_pronouns as choice_pronouns,
    you_medicaid as you_medicaid,
    add_family_member_16 as add_family_member_16,
    email_address as email_address,
    add_family_member_18 as add_family_member_18,
    medicaid_number as medicaid_number,
    other_best_represents as other_best_represents,
    other_biological_sex as other_biological_sex,
    other_gender_identity as other_gender_identity,
    other_pronouns as other_pronouns,
    other_you_first as other_you_first,
    phone_number as phone_number,
    physical_address as physical_address,
    you_first_learn as you_first_learn,
    zip_code as zip_code,
    CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_registration_info') }}