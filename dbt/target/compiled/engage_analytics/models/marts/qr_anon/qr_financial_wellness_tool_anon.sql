

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
    fin_there_you as fin_there_you,
    fin_you_not_pay as fin_you_not_pay,
    pay_basics_difficulty as pay_basics_difficulty,
    fin_strongly_you_agree as fin_strongly_you_agree,
    fin_you_move_other as fin_you_move_other,
    fin_you_run_out as fin_you_run_out,
    shelter_stay as shelter_stay,
    fin_you_not_pay_8 as fin_you_not_pay_8,
    food_insecurity as food_insecurity,
    fin_food_bought_just as fin_food_bought_just,
    fin_your_phone_gas as fin_your_phone_gas,
    fin_you_think_your as fin_you_think_your,
    financial_wellness_tool as financial_wellness_tool,
    financial_wellness_tool_14 as financial_wellness_tool_14,
    financial_wellness_tool_15 as financial_wellness_tool_15,
    financial_wellness_tool_16 as financial_wellness_tool_16,
    financial_wellness_tool_17 as financial_wellness_tool_17,
    financial_wellness_tool_18 as financial_wellness_tool_18,
    financial_wellness_tool_19 as financial_wellness_tool_19,
    financial_wellness_tool_20 as financial_wellness_tool_20,
    financial_wellness_tool_21 as financial_wellness_tool_21,
    financial_wellness_tool_22 as financial_wellness_tool_22,
    financial_wellness_tool_23 as financial_wellness_tool_23,
    CURRENT_TIMESTAMP as anonymized_at

from "airbyte"."engage_analytics_engage_analytics_mart"."qr_financial_wellness_tool"