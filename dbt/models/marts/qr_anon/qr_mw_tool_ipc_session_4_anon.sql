{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_mw_tool_ipc_session_4
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
    mental_wellness_tool as mental_wellness_tool,
    mental_wellness_tool_2 as mental_wellness_tool_2,
    mental_wellness_tool_3 as mental_wellness_tool_3,
    mental_wellness_tool_4 as mental_wellness_tool_4,
    mental_wellness_tool_5 as mental_wellness_tool_5,
    mental_wellness_tool_6 as mental_wellness_tool_6,
    mental_wellness_tool_7 as mental_wellness_tool_7,
    mental_wellness_tool_8 as mental_wellness_tool_8,
    mental_wellness_tool_9 as mental_wellness_tool_9,
    mental_wellness_tool_10 as mental_wellness_tool_10,
    mental_wellness_tool_11 as mental_wellness_tool_11,
    mental_wellness_tool_12 as mental_wellness_tool_12,
    mental_wellness_tool_13 as mental_wellness_tool_13,
    mental_wellness_tool_14 as mental_wellness_tool_14,
    mental_wellness_tool_15 as mental_wellness_tool_15,
    mental_wellness_tool_16 as mental_wellness_tool_16,
    mental_wellness_tool_17 as mental_wellness_tool_17,
    mental_wellness_tool_18 as mental_wellness_tool_18,
    'REDACTED' as patient_age,
    'REDACTED' as patient_biological_sex,
    'REDACTED' as patient_dob,
    'REDACTED' as patient_gender_identity,
    'REDACTED' as patient_how_you_think_of_yourself,
    'REDACTED' as patient_name,
    'REDACTED' as patient_pronouns,
    mental_wellness_tool_26 as mental_wellness_tool_26,
    mental_wellness_tool_27 as mental_wellness_tool_27,
    mental_wellness_tool_28 as mental_wellness_tool_28,
    CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_mw_tool_ipc_session_4') }}