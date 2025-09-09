{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_financial_wellness_tool with PII fields masked based on questionnaire_metadata.anon flag
-- Questionnaire: Financial Wellness Tool (Questionnaire/q-financial-wellness-tool)
-- PII fields masked: 4 fields

select 
    MD5(COALESCE(qr_id, '')::text) as qr_id_hash,
    questionnaire_id,
    MD5(COALESCE(subject_patient_id, '')::text) as subject_patient_id_hash,
    encounter_id,
    author_practitioner_id,
    practitioner_location_id,
    practitioner_organization_id,
    practitioner_id,
    practitioner_careteam_id,
    application_version,
        financial_in_the_past_12_months_was_there_a_time_when_you_or,
        'REDACTED' as financial_in_the_past_12_months_did_you_not_pay_the_full_amo,
        'REDACTED' as financial_how_hard_is_it_for_you_to_pay_for_the_very_basics_,
        financial_how_strongly_do_you_agree_or_disagree_with_the_fol,
        financial_in_the_past_12_months_did_you_move_in_with_other_p,
        financial_in_the_past_12_months_how_often_did_you_run_out_of,
        'REDACTED' as financial_in_the_past_12_months_did_you_stay_at_a_shelter_in,
        financial_in_the_past_12_months_did_you_not_pay_the_full_amo_8,
        financial_i_worried_whether_my_food_would_run_out_before_i_g,
        financial_the_food_that_i_bought_just_didnt_last_and_i_didnt,
        CASE 
        WHEN financial_in_the_past_12_months_was_your_phone_gas_heating_w IS NOT NULL AND LENGTH(financial_in_the_past_12_months_was_your_phone_gas_heating_w::text) >= 4
        THEN 'XXX-XXX-' || RIGHT(financial_in_the_past_12_months_was_your_phone_gas_heating_w::text, 4)
        WHEN financial_in_the_past_12_months_was_your_phone_gas_heating_w IS NOT NULL
        THEN 'XXX-XXX-' || financial_in_the_past_12_months_was_your_phone_gas_heating_w::text
        ELSE 'NO PHONE'
    END as financial_in_the_past_12_months_was_your_phone_gas_heating_w,
        financial_when_you_think_about_your_financial_situation_how_,
        "high-debt",
        "high-food-insecurity",
        "high-housing-insecurity",
        "high-paying-bills",
        "moderate-debt",
        "moderate-food-insecurity",
        "moderate-housing-insecurity",
        "moderate-medical-hardships",
        "moderate-paying-bills",
        "no-financial-hardship",
        "overall-financial-hardship",
        CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_financial_wellness_tool') }}