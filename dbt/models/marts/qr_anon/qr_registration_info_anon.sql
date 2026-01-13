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
    add_family_member_registration as add_family_member_registration,
    add_family_member_registration_middle_name as add_family_member_registration_middle_name,
    add_family_member_registration_which_of_the_following_best_repr as add_family_member_registration_which_of_the_following_best_repr,
    add_family_member_registration_age_estimate_if_unknown as add_family_member_registration_age_estimate_if_unknown,
    add_family_member_registration_last_name as add_family_member_registration_last_name,
    age as age,
    add_family_member_registration_calculated_age_6 as add_family_member_registration_calculated_age_6,
    add_family_member_registration_unique_id as add_family_member_registration_unique_id,
    add_family_member_registration_have_you_received_engage_service as add_family_member_registration_have_you_received_engage_service,
    add_family_member_registration_calculated_month_6 as add_family_member_registration_calculated_month_6,
    add_family_member_registration_calculated_year_6 as add_family_member_registration_calculated_year_6,
    add_family_member_registration_date_of_birth_6 as add_family_member_registration_date_of_birth_6,
    add_family_member_registration_what_term_best_expresses_how_you as add_family_member_registration_what_term_best_expresses_how_you,
    add_family_member_registration_what_are_your_preferred_pronouns as add_family_member_registration_what_are_your_preferred_pronouns,
    add_family_member_registration_do_you_have_medicaid as add_family_member_registration_do_you_have_medicaid,
    add_family_member_registration_first_name as add_family_member_registration_first_name,
    add_family_member_registration_email_address as add_family_member_registration_email_address,
    add_family_member_registration_what_was_your_biological_sex_at_ as add_family_member_registration_what_was_your_biological_sex_at_,
    add_family_member_registration_medicaid_number as add_family_member_registration_medicaid_number,
    add_family_member_registration_2 as add_family_member_registration_2,
    add_family_member_registration_3 as add_family_member_registration_3,
    add_family_member_registration_4 as add_family_member_registration_4,
    add_family_member_registration_5 as add_family_member_registration_5,
    add_family_member_registration_6 as add_family_member_registration_6,
    add_family_member_registration_phone_number as add_family_member_registration_phone_number,
    add_family_member_registration_physical_address as add_family_member_registration_physical_address,
    add_family_member_registration_where_did_you_first_learn_about_ as add_family_member_registration_where_did_you_first_learn_about_,
    add_family_member_registration_zip_code as add_family_member_registration_zip_code,
    CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_registration_info') }}