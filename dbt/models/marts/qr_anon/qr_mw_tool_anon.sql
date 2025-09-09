{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_mw_tool with PII fields masked based on questionnaire_metadata.anon flag
-- Auto-generated to mask fields marked as anon=TRUE in questionnaire_metadata

with metadata_pii as (
    select distinct
        question_alias
    from {{ ref('questionnaire_metadata') }}
    where questionnaire_id in ('Questionnaire/1613532')
    and anon = 'TRUE'
),

source_data as (
    select * from {{ ref('qr_mw_tool') }}
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
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'mental_in_the_past_year_have_there_been_times_when_you_fe')
        THEN 'REDACTED'
        ELSE mental_in_the_past_year_have_there_been_times_when_you_fe::text
    END as mental_in_the_past_year_have_there_been_times_when_you_fe,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'mental_in_the_past_year_have_you_ever_felt_that_your_thou')
        THEN 'REDACTED'
        ELSE mental_in_the_past_year_have_you_ever_felt_that_your_thou::text
    END as mental_in_the_past_year_have_you_ever_felt_that_your_thou,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'mental_in_the_past_year_how_often_do_you_have_a_drink_con')
        THEN 'REDACTED'
        ELSE mental_in_the_past_year_how_often_do_you_have_a_drink_con::text
    END as mental_in_the_past_year_how_often_do_you_have_a_drink_con,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'mental_in_the_past_month_have_you_wished_you_were_dead_or')
        THEN 'REDACTED'
        ELSE mental_in_the_past_month_have_you_wished_you_were_dead_or::text
    END as mental_in_the_past_month_have_you_wished_you_were_dead_or,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'mental_in_the_past_year_did_you_at_any_time_hear_voices_s')
        THEN 'REDACTED'
        ELSE mental_in_the_past_year_did_you_at_any_time_hear_voices_s::text
    END as mental_in_the_past_year_did_you_at_any_time_hear_voices_s,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'mental_q2_depressed')
        THEN 'REDACTED'
        ELSE mental_q2_depressed::text
    END as mental_q2_depressed,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'mental_in_the_past_year_how_many_times_have_you_used_a_re')
        THEN 'REDACTED'
        ELSE mental_in_the_past_year_how_many_times_have_you_used_a_re::text
    END as mental_in_the_past_year_how_many_times_have_you_used_a_re,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'mental_in_the_past_year_how_many_drinks_containing_alcoho')
        THEN 'REDACTED'
        ELSE mental_in_the_past_year_how_many_drinks_containing_alcoho::text
    END as mental_in_the_past_year_how_many_drinks_containing_alcoho,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'mental_in_the_past_3_months_have_you_ever_done_anything_s')
        THEN 'REDACTED'
        ELSE mental_in_the_past_3_months_have_you_ever_done_anything_s::text
    END as mental_in_the_past_3_months_have_you_ever_done_anything_s,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'mental_q1_anxious')
        THEN 'REDACTED'
        ELSE mental_q1_anxious::text
    END as mental_q1_anxious,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'mental_in_the_past_month_have_you_had_any_actual_thoughts')
        THEN 'REDACTED'
        ELSE mental_in_the_past_month_have_you_had_any_actual_thoughts::text
    END as mental_in_the_past_month_have_you_had_any_actual_thoughts,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'mental_q5_restless')
        THEN 'REDACTED'
        ELSE mental_q5_restless::text
    END as mental_q5_restless,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'mental_in_the_past_year_have_there_been_times_when_you_fe_13')
        THEN 'REDACTED'
        ELSE mental_in_the_past_year_have_there_been_times_when_you_fe_13::text
    END as mental_in_the_past_year_have_there_been_times_when_you_fe_13,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'mental_did_you_discuss_this_clients_severe_mental_health_')
        THEN 'REDACTED'
        ELSE mental_did_you_discuss_this_clients_severe_mental_health_::text
    END as mental_did_you_discuss_this_clients_severe_mental_health_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'mental_supervisors_name')
        THEN 'REDACTED'
        ELSE mental_supervisors_name::text
    END as mental_supervisors_name,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'mental_what_is_the_recommended_plan_to_address_the_probab')
        THEN 'REDACTED'
        ELSE mental_what_is_the_recommended_plan_to_address_the_probab::text
    END as mental_what_is_the_recommended_plan_to_address_the_probab,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'mental_referral_details_if_the_client_is_referred_out')
        THEN 'REDACTED'
        ELSE mental_referral_details_if_the_client_is_referred_out::text
    END as mental_referral_details_if_the_client_is_referred_out,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'retake_screening')
        THEN 'REDACTED'
        ELSE retake_screening::text
    END as retake_screening,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'retake_screening_19')
        THEN 'REDACTED'
        ELSE retake_screening_19::text
    END as retake_screening_19,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'alcohol-problem')
        THEN 'REDACTED'
        ELSE "alcohol-problem"::text
    END as "alcohol-problem",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'common-mental-health')
        THEN 'REDACTED'
        ELSE "common-mental-health"::text
    END as "common-mental-health",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'drug-problem')
        THEN 'REDACTED'
        ELSE "drug-problem"::text
    END as "drug-problem",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'how-many-drinks')
        THEN 'REDACTED'
        ELSE "how-many-drinks"::text
    END as "how-many-drinks",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'no-mental-problem')
        THEN 'REDACTED'
        ELSE "no-mental-problem"::text
    END as "no-mental-problem",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'patient-age')
        THEN 'REDACTED'
        ELSE "patient-age"::text
    END as "patient-age",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'patient-biological-sex')
        THEN 'REDACTED'
        ELSE "patient-biological-sex"::text
    END as "patient-biological-sex",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'patient-dob')
        THEN 'REDACTED'
        ELSE "patient-dob"::text
    END as "patient-dob",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'patient-gender-identity')
        THEN 'REDACTED'
        ELSE "patient-gender-identity"::text
    END as "patient-gender-identity",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'patient-how-you-think-of-yourself')
        THEN 'REDACTED'
        ELSE "patient-how-you-think-of-yourself"::text
    END as "patient-how-you-think-of-yourself",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'patient-name')
        THEN 'REDACTED'
        ELSE "patient-name"::text
    END as "patient-name",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'patient-pronouns')
        THEN 'REDACTED'
        ELSE "patient-pronouns"::text
    END as "patient-pronouns",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'q1-to-q3-negative')
        THEN 'REDACTED'
        ELSE "q1-to-q3-negative"::text
    END as "q1-to-q3-negative",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'severe-mental-health')
        THEN 'REDACTED'
        ELSE "severe-mental-health"::text
    END as "severe-mental-health",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'suicide-risk')
        THEN 'REDACTED'
        ELSE "suicide-risk"::text
    END as "suicide-risk",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-id-common-mental-health-form')
        THEN 'REDACTED'
        ELSE "task-id-common-mental-health-form"::text
    END as "task-id-common-mental-health-form",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-id-mwtool-pdf')
        THEN 'REDACTED'
        ELSE "task-id-mwtool-pdf"::text
    END as "task-id-mwtool-pdf",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-id-socio-pdf')
        THEN 'REDACTED'
        ELSE "task-id-socio-pdf"::text
    END as "task-id-socio-pdf",
        CURRENT_TIMESTAMP as anonymized_at

from source_data