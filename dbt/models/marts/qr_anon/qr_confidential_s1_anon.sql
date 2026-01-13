{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_confidential_s1
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
    add_family_member_registration_calculated_age as add_family_member_registration_calculated_age,
    add_family_member_registration_calculated_month as add_family_member_registration_calculated_month,
    add_family_member_registration_calculated_year as add_family_member_registration_calculated_year,
    add_family_member_registration_date_of_birth as add_family_member_registration_date_of_birth,
    CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_confidential_s1') }}