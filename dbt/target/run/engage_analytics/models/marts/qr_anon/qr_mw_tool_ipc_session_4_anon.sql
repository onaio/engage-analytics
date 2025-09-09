
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_mw_tool_ipc_session_4_anon__dbt_tmp"
    
    
  as (
    

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
    alcohol_problem as alcohol_problem,
    mental_wellness_tool_13 as mental_wellness_tool_13,
    common_mental_health as common_mental_health,
    drug_problem as drug_problem,
    mental_wellness_tool_16 as mental_wellness_tool_16,
    many_drinks as many_drinks,
    mental_problem as mental_problem,
    'REDACTED' as patient_age,
    'REDACTED' as patient_biological_sex,
    'REDACTED' as patient_dob,
    'REDACTED' as patient_gender_identity,
    'REDACTED' as patient_how_you_think_of_yourself,
    'REDACTED' as patient_name,
    'REDACTED' as patient_pronouns,
    severe_mental_health as severe_mental_health,
    suicide_risk as suicide_risk,
    task_tool_ipc as task_tool_ipc,
    CURRENT_TIMESTAMP as anonymized_at

from "airbyte"."engage_analytics_engage_analytics_mart"."qr_mw_tool_ipc_session_4"
  );