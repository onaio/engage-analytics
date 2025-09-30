
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_mw_tool_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_mw_tool
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
    discussion_supervisor_severe as discussion_supervisor_severe,
    discussion_supervisor_severe_7 as discussion_supervisor_severe_7,
    mental_wellness_tool_8 as mental_wellness_tool_8,
    mental_wellness_tool_9 as mental_wellness_tool_9,
    discussion_supervisor_incomple as discussion_supervisor_incomple,
    discussion_supervisor_incomple_11 as discussion_supervisor_incomple_11,
    discussion_supervisor_severe_12 as discussion_supervisor_severe_12,
    mental_wellness_tool_13 as mental_wellness_tool_13,
    mental_wellness_tool_14 as mental_wellness_tool_14,
    mental_wellness_tool_15 as mental_wellness_tool_15,
    mental_wellness_tool_16 as mental_wellness_tool_16,
    mental_wellness_tool_17 as mental_wellness_tool_17,
    mental_wellness_tool_18 as mental_wellness_tool_18,
    mental_wellness_tool_19 as mental_wellness_tool_19,
    mental_wellness_tool_20 as mental_wellness_tool_20,
    mental_wellness_tool_21 as mental_wellness_tool_21,
    discussion_supervisor_severe_22 as discussion_supervisor_severe_22,
    mental_wellness_tool_23 as mental_wellness_tool_23,
    mental_wellness_tool_24 as mental_wellness_tool_24,
    'REDACTED' as patient_age,
    'REDACTED' as patient_biological_sex,
    'REDACTED' as patient_dob,
    'REDACTED' as patient_gender_identity,
    'REDACTED' as patient_how_you_think_of_yourself,
    'REDACTED' as patient_name,
    'REDACTED' as patient_pronouns,
    client_mental_wellness as client_mental_wellness,
    mental_wellness_tool_33 as mental_wellness_tool_33,
    mental_wellness_tool_34 as mental_wellness_tool_34,
    mental_wellness_tool_35 as mental_wellness_tool_35,
    mental_wellness_tool_36 as mental_wellness_tool_36,
    task_socio_pdf as task_socio_pdf,
    CURRENT_TIMESTAMP as anonymized_at

from "airbyte"."engage_analytics_engage_analytics_mart"."qr_mw_tool"
  );