{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_financial_wellness_tool
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
    financial_wellness_tool_in_the_past_12_months_was_there_a_time_ as financial_wellness_tool_in_the_past_12_months_was_there_a_time_,
    financial_wellness_tool_in_the_past_12_months_did_you_not_pay_t as financial_wellness_tool_in_the_past_12_months_did_you_not_pay_t,
    financial_wellness_tool_how_hard_is_it_for_you_to_pay_for_the_v as financial_wellness_tool_how_hard_is_it_for_you_to_pay_for_the_v,
    financial_wellness_tool_how_strongly_do_you_agree_or_disagree_w as financial_wellness_tool_how_strongly_do_you_agree_or_disagree_w,
    financial_wellness_tool_in_the_past_12_months_did_you_move_in_w as financial_wellness_tool_in_the_past_12_months_did_you_move_in_w,
    financial_wellness_tool_in_the_past_12_months_how_often_did_you as financial_wellness_tool_in_the_past_12_months_how_often_did_you,
    financial_wellness_tool_in_the_past_12_months_did_you_stay_at_a as financial_wellness_tool_in_the_past_12_months_did_you_stay_at_a,
    financial_wellness_tool_in_the_past_12_months_did_you_not_pay_2 as financial_wellness_tool_in_the_past_12_months_did_you_not_pay_2,
    financial_wellness_tool_i_worried_whether_my_food_would_run_out as financial_wellness_tool_i_worried_whether_my_food_would_run_out,
    financial_wellness_tool_the_food_that_i_bought_just_didn_t_last as financial_wellness_tool_the_food_that_i_bought_just_didn_t_last,
    financial_wellness_tool_in_the_past_12_months_was_your_phone_ga as financial_wellness_tool_in_the_past_12_months_was_your_phone_ga,
    financial_wellness_tool_when_you_think_about_your_financial_sit as financial_wellness_tool_when_you_think_about_your_financial_sit,
    financial_wellness_tool as financial_wellness_tool,
    financial_wellness_tool_2 as financial_wellness_tool_2,
    financial_wellness_tool_3 as financial_wellness_tool_3,
    financial_wellness_tool_4 as financial_wellness_tool_4,
    financial_wellness_tool_5 as financial_wellness_tool_5,
    financial_wellness_tool_6 as financial_wellness_tool_6,
    financial_wellness_tool_7 as financial_wellness_tool_7,
    financial_wellness_tool_8 as financial_wellness_tool_8,
    financial_wellness_tool_9 as financial_wellness_tool_9,
    financial_wellness_tool_10 as financial_wellness_tool_10,
    financial_wellness_tool_11 as financial_wellness_tool_11,
    CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_financial_wellness_tool') }}