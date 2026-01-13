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
    mental_wellness_tool_in_the_past_year_have_there_been_times_w_3 as mental_wellness_tool_in_the_past_year_have_there_been_times_w_3,
    mental_wellness_tool_in_the_past_year_have_you_ever_felt_that_2 as mental_wellness_tool_in_the_past_year_have_you_ever_felt_that_2,
    mental_wellness_tool_in_the_past_year_how_often_do_you_have_a_2 as mental_wellness_tool_in_the_past_year_how_often_do_you_have_a_2,
    mental_wellness_tool_in_the_past_month_have_you_wished_you_we_2 as mental_wellness_tool_in_the_past_month_have_you_wished_you_we_2,
    mental_wellness_tool_in_the_past_year_did_you_at_any_time_hea_2 as mental_wellness_tool_in_the_past_year_did_you_at_any_time_hea_2,
    mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_4 as mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_4,
    mental_wellness_tool_in_the_past_year_how_many_times_have_you_2 as mental_wellness_tool_in_the_past_year_how_many_times_have_you_2,
    mental_wellness_tool_in_the_past_year_how_many_drinks_contain_2 as mental_wellness_tool_in_the_past_year_how_many_drinks_contain_2,
    mental_wellness_tool_in_the_past_3_months_have_you_ever_done__2 as mental_wellness_tool_in_the_past_3_months_have_you_ever_done__2,
    mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_5 as mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_5,
    mental_wellness_tool_in_the_past_month_have_you_had_any_actua_2 as mental_wellness_tool_in_the_past_month_have_you_had_any_actua_2,
    mental_wellness_tool_alcohol_problem_2 as mental_wellness_tool_alcohol_problem_2,
    mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_6 as mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_6,
    mental_wellness_tool_common_mental_health_2 as mental_wellness_tool_common_mental_health_2,
    mental_wellness_tool_drug_problem_2 as mental_wellness_tool_drug_problem_2,
    mental_wellness_tool_in_the_past_year_have_there_been_times_w_4 as mental_wellness_tool_in_the_past_year_have_there_been_times_w_4,
    mental_wellness_tool_how_many_drinks_2 as mental_wellness_tool_how_many_drinks_2,
    mental_wellness_tool_no_mental_problem_2 as mental_wellness_tool_no_mental_problem_2,
    'REDACTED' as patient_age_3,
    'REDACTED' as patient_biological_sex_2,
    'REDACTED' as patient_date_of_birth_3,
    'REDACTED' as patient_gender_identity_2,
    'REDACTED' as patient_how_you_think_of_yourself_2,
    'REDACTED' as patient_name_3,
    'REDACTED' as patient_pronouns_2,
    mental_wellness_tool_severe_mental_health_2 as mental_wellness_tool_severe_mental_health_2,
    mental_wellness_tool_suicide_risk_2 as mental_wellness_tool_suicide_risk_2,
    mental_wellness_tool_taskid_of_pdf_2 as mental_wellness_tool_taskid_of_pdf_2,
    CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_mw_tool_ipc_session_4') }}