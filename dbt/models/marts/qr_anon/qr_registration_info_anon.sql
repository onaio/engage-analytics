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
    add_family_member_13 as add_family_member_13,
    add_family_member_14 as add_family_member_14,
    add_family_member_15 as add_family_member_15,
    add_family_member_16 as add_family_member_16,
    add_family_member_17 as add_family_member_17,
    add_family_member_18 as add_family_member_18,
    add_family_member_19 as add_family_member_19,
    add_family_member_20 as add_family_member_20,
    add_family_member_21 as add_family_member_21,
    add_family_member_22 as add_family_member_22,
    add_family_member_23 as add_family_member_23,
    add_family_member_24 as add_family_member_24,
    add_family_member_25 as add_family_member_25,
    add_family_member_26 as add_family_member_26,
    add_family_member_27 as add_family_member_27,
    add_family_member_28 as add_family_member_28,
    CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_registration_info') }}