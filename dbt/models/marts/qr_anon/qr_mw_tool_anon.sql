{{
    config(
        materialized='view'
    )
}}

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
    mental_wellness_tool_in_the_past_year_have_there_been_times_whe as mental_wellness_tool_in_the_past_year_have_there_been_times_whe,
    mental_wellness_tool_in_the_past_year_have_you_ever_felt_that_y as mental_wellness_tool_in_the_past_year_have_you_ever_felt_that_y,
    mental_wellness_tool_in_the_past_year_how_often_do_you_have_a_d as mental_wellness_tool_in_the_past_year_how_often_do_you_have_a_d,
    mental_wellness_tool_in_the_past_month_have_you_wished_you_were as mental_wellness_tool_in_the_past_month_have_you_wished_you_were,
    mental_wellness_tool_in_the_past_year_did_you_at_any_time_hear_ as mental_wellness_tool_in_the_past_year_did_you_at_any_time_hear_,
    discussion_with_supervisor_for_severe_mental_health_did_you_dis as discussion_with_supervisor_for_severe_mental_health_did_you_dis,
    discussion_with_supervisor_for_severe_mental_health_what_is_the as discussion_with_supervisor_for_severe_mental_health_what_is_the,
    mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_bee as mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_bee,
    mental_wellness_tool_in_the_past_year_how_many_times_have_you_u as mental_wellness_tool_in_the_past_year_how_many_times_have_you_u,
    discussion_with_supervisor_for_incomplete_answers_since_respons as discussion_with_supervisor_for_incomplete_answers_since_respons,
    discussion_with_supervisor_for_incomplete_answers_since_respo_2 as discussion_with_supervisor_for_incomplete_answers_since_respo_2,
    discussion_with_supervisor_for_severe_mental_health_referral_de as discussion_with_supervisor_for_severe_mental_health_referral_de,
    mental_wellness_tool_in_the_past_year_how_many_drinks_containin as mental_wellness_tool_in_the_past_year_how_many_drinks_containin,
    mental_wellness_tool_in_the_past_3_months_have_you_ever_done_an as mental_wellness_tool_in_the_past_3_months_have_you_ever_done_an,
    mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_2 as mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_2,
    mental_wellness_tool_in_the_past_month_have_you_had_any_actual_ as mental_wellness_tool_in_the_past_month_have_you_had_any_actual_,
    mental_wellness_tool_alcohol_problem as mental_wellness_tool_alcohol_problem,
    mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_3 as mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_3,
    mental_wellness_tool_common_mental_health as mental_wellness_tool_common_mental_health,
    mental_wellness_tool_drug_problem as mental_wellness_tool_drug_problem,
    mental_wellness_tool_in_the_past_year_have_there_been_times_w_2 as mental_wellness_tool_in_the_past_year_have_there_been_times_w_2,
    discussion_with_supervisor_for_severe_mental_health_supervisor_ as discussion_with_supervisor_for_severe_mental_health_supervisor_,
    mental_wellness_tool_how_many_drinks as mental_wellness_tool_how_many_drinks,
    mental_wellness_tool_no_mental_problem as mental_wellness_tool_no_mental_problem,
    'REDACTED' as patient_age_2,
    'REDACTED' as patient_biological_sex,
    'REDACTED' as patient_date_of_birth_2,
    'REDACTED' as patient_gender_identity,
    'REDACTED' as patient_how_you_think_of_yourself,
    'REDACTED' as patient_name_2,
    'REDACTED' as patient_pronouns,
    the_client_s_mental_wellness_tool_results_q1_to_q3_negative as the_client_s_mental_wellness_tool_results_q1_to_q3_negative,
    mental_wellness_tool_severe_mental_health as mental_wellness_tool_severe_mental_health,
    mental_wellness_tool_suicide_risk as mental_wellness_tool_suicide_risk,
    mental_wellness_tool_taskid_of_common_mental_health_form as mental_wellness_tool_taskid_of_common_mental_health_form,
    mental_wellness_tool_taskid_of_pdf as mental_wellness_tool_taskid_of_pdf,
    task_id_socio_pdf as task_id_socio_pdf,
    CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_mw_tool') }}