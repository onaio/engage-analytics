{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_mood_rating_s2
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
    mood_rating_ipc_session_2_total_score as mood_rating_ipc_session_2_total_score,
    add_family_member_registration_calculated_age_4 as add_family_member_registration_calculated_age_4,
    add_family_member_registration_calculated_month_4 as add_family_member_registration_calculated_month_4,
    add_family_member_registration_calculated_year_4 as add_family_member_registration_calculated_year_4,
    add_family_member_registration_date_of_birth_4 as add_family_member_registration_date_of_birth_4,
    mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being__2 as mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being__2,
    CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_mood_rating_s2') }}