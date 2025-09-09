{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_financial_wellness_tool with PII fields masked based on questionnaire_metadata.anon flag
-- Auto-generated to mask fields marked as anon=TRUE in questionnaire_metadata

with metadata_pii as (
    select distinct
        question_alias
    from {{ ref('questionnaire_metadata') }}
    where questionnaire_id in ('Questionnaire/q-financial-wellness-tool')
    and anon = 'TRUE'
),

source_data as (
    select * from {{ ref('qr_financial_wellness_tool') }}
)

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
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'financial_in_the_past_12_months_was_there_a_time_when_you_or')
        THEN 'REDACTED'
        ELSE financial_in_the_past_12_months_was_there_a_time_when_you_or::text
    END as financial_in_the_past_12_months_was_there_a_time_when_you_or,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'financial_in_the_past_12_months_did_you_not_pay_the_full_amo')
        THEN 'REDACTED'
        ELSE financial_in_the_past_12_months_did_you_not_pay_the_full_amo::text
    END as financial_in_the_past_12_months_did_you_not_pay_the_full_amo,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'financial_how_hard_is_it_for_you_to_pay_for_the_very_basics_')
        THEN 'REDACTED'
        ELSE financial_how_hard_is_it_for_you_to_pay_for_the_very_basics_::text
    END as financial_how_hard_is_it_for_you_to_pay_for_the_very_basics_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'financial_how_strongly_do_you_agree_or_disagree_with_the_fol')
        THEN 'REDACTED'
        ELSE financial_how_strongly_do_you_agree_or_disagree_with_the_fol::text
    END as financial_how_strongly_do_you_agree_or_disagree_with_the_fol,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'financial_in_the_past_12_months_did_you_move_in_with_other_p')
        THEN 'REDACTED'
        ELSE financial_in_the_past_12_months_did_you_move_in_with_other_p::text
    END as financial_in_the_past_12_months_did_you_move_in_with_other_p,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'financial_in_the_past_12_months_how_often_did_you_run_out_of')
        THEN 'REDACTED'
        ELSE financial_in_the_past_12_months_how_often_did_you_run_out_of::text
    END as financial_in_the_past_12_months_how_often_did_you_run_out_of,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'financial_in_the_past_12_months_did_you_stay_at_a_shelter_in')
        THEN 'REDACTED'
        ELSE financial_in_the_past_12_months_did_you_stay_at_a_shelter_in::text
    END as financial_in_the_past_12_months_did_you_stay_at_a_shelter_in,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'financial_in_the_past_12_months_did_you_not_pay_the_full_amo_8')
        THEN 'REDACTED'
        ELSE financial_in_the_past_12_months_did_you_not_pay_the_full_amo_8::text
    END as financial_in_the_past_12_months_did_you_not_pay_the_full_amo_8,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'financial_i_worried_whether_my_food_would_run_out_before_i_g')
        THEN 'REDACTED'
        ELSE financial_i_worried_whether_my_food_would_run_out_before_i_g::text
    END as financial_i_worried_whether_my_food_would_run_out_before_i_g,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'financial_the_food_that_i_bought_just_didnt_last_and_i_didnt')
        THEN 'REDACTED'
        ELSE financial_the_food_that_i_bought_just_didnt_last_and_i_didnt::text
    END as financial_the_food_that_i_bought_just_didnt_last_and_i_didnt,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'financial_in_the_past_12_months_was_your_phone_gas_heating_w')
        THEN 
            CASE 
                WHEN financial_in_the_past_12_months_was_your_phone_gas_heating_w IS NOT NULL AND LENGTH(financial_in_the_past_12_months_was_your_phone_gas_heating_w::text) >= 4
                THEN 'XXX-XXX-' || RIGHT(financial_in_the_past_12_months_was_your_phone_gas_heating_w::text, 4)
                ELSE 'NO PHONE'
            END
        ELSE financial_in_the_past_12_months_was_your_phone_gas_heating_w::text
    END as financial_in_the_past_12_months_was_your_phone_gas_heating_w,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'financial_when_you_think_about_your_financial_situation_how_')
        THEN 'REDACTED'
        ELSE financial_when_you_think_about_your_financial_situation_how_::text
    END as financial_when_you_think_about_your_financial_situation_how_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'high-debt')
        THEN 'REDACTED'
        ELSE "high-debt"::text
    END as "high-debt",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'high-food-insecurity')
        THEN 'REDACTED'
        ELSE "high-food-insecurity"::text
    END as "high-food-insecurity",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'high-housing-insecurity')
        THEN 'REDACTED'
        ELSE "high-housing-insecurity"::text
    END as "high-housing-insecurity",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'high-paying-bills')
        THEN 'REDACTED'
        ELSE "high-paying-bills"::text
    END as "high-paying-bills",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'moderate-debt')
        THEN 'REDACTED'
        ELSE "moderate-debt"::text
    END as "moderate-debt",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'moderate-food-insecurity')
        THEN 'REDACTED'
        ELSE "moderate-food-insecurity"::text
    END as "moderate-food-insecurity",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'moderate-housing-insecurity')
        THEN 'REDACTED'
        ELSE "moderate-housing-insecurity"::text
    END as "moderate-housing-insecurity",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'moderate-medical-hardships')
        THEN 'REDACTED'
        ELSE "moderate-medical-hardships"::text
    END as "moderate-medical-hardships",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'moderate-paying-bills')
        THEN 'REDACTED'
        ELSE "moderate-paying-bills"::text
    END as "moderate-paying-bills",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'no-financial-hardship')
        THEN 'REDACTED'
        ELSE "no-financial-hardship"::text
    END as "no-financial-hardship",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'overall-financial-hardship')
        THEN 'REDACTED'
        ELSE "overall-financial-hardship"::text
    END as "overall-financial-hardship",
        CURRENT_TIMESTAMP as anonymized_at

from source_data