{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_mw_tool_ipc_session_4 with PII fields masked based on questionnaire_metadata.anon flag
-- Auto-generated to mask fields marked as anon=TRUE in questionnaire_metadata

with metadata_pii as (
    select distinct
        question_alias
    from {{ ref('questionnaire_metadata') }}
    where questionnaire_id in ('Questionnaire/mw-tool-ipc-session-4')
    and anon = 'TRUE'
),

source_data as (
    select * from {{ ref('qr_mw_tool_ipc_session_4') }}
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
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'in_the_past_year_have_there_been_times_when_you_felt_that_s')
        THEN 'REDACTED'
        ELSE in_the_past_year_have_there_been_times_when_you_felt_that_s::text
    END as in_the_past_year_have_there_been_times_when_you_felt_that_s,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'in_the_past_year_have_you_ever_felt_that_your_thoughts_were')
        THEN 'REDACTED'
        ELSE in_the_past_year_have_you_ever_felt_that_your_thoughts_were::text
    END as in_the_past_year_have_you_ever_felt_that_your_thoughts_were,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'in_the_past_year_how_often_do_you_have_a_drink_containing_a')
        THEN 'REDACTED'
        ELSE in_the_past_year_how_often_do_you_have_a_drink_containing_a::text
    END as in_the_past_year_how_often_do_you_have_a_drink_containing_a,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'in_the_past_month_have_you_wished_you_were_dead_or_wished_y')
        THEN 'REDACTED'
        ELSE in_the_past_month_have_you_wished_you_were_dead_or_wished_y::text
    END as in_the_past_month_have_you_wished_you_were_dead_or_wished_y,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'in_the_past_year_did_you_at_any_time_hear_voices_saying_qui')
        THEN 'REDACTED'
        ELSE in_the_past_year_did_you_at_any_time_hear_voices_saying_qui::text
    END as in_the_past_year_did_you_at_any_time_hear_voices_saying_qui,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'in_the_last_2_weeks_how_often_have_you_been_feeling_down_d')
        THEN 'REDACTED'
        ELSE in_the_last_2_weeks_how_often_have_you_been_feeling_down_d::text
    END as in_the_last_2_weeks_how_often_have_you_been_feeling_down_d,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'in_the_past_year_how_many_times_have_you_used_a_recreationa')
        THEN 'REDACTED'
        ELSE in_the_past_year_how_many_times_have_you_used_a_recreationa::text
    END as in_the_past_year_how_many_times_have_you_used_a_recreationa,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'in_the_past_year_how_many_drinks_containing_alcohol_do_you')
        THEN 'REDACTED'
        ELSE in_the_past_year_how_many_drinks_containing_alcohol_do_you::text
    END as in_the_past_year_how_many_drinks_containing_alcohol_do_you,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'in_the_past_3_months_have_you_ever_done_anything_started_t')
        THEN 'REDACTED'
        ELSE in_the_past_3_months_have_you_ever_done_anything_started_t::text
    END as in_the_past_3_months_have_you_ever_done_anything_started_t,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'in_the_last_2_weeks_how_often_have_you_been_feeling_nervous')
        THEN 'REDACTED'
        ELSE in_the_last_2_weeks_how_often_have_you_been_feeling_nervous::text
    END as in_the_last_2_weeks_how_often_have_you_been_feeling_nervous,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'in_the_past_month_have_you_had_any_actual_thoughts_of_killi')
        THEN 'REDACTED'
        ELSE in_the_past_month_have_you_had_any_actual_thoughts_of_killi::text
    END as in_the_past_month_have_you_had_any_actual_thoughts_of_killi,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'in_the_last_2_weeks_how_often_have_you_been_so_restless_tha')
        THEN 'REDACTED'
        ELSE in_the_last_2_weeks_how_often_have_you_been_so_restless_tha::text
    END as in_the_last_2_weeks_how_often_have_you_been_so_restless_tha,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'in_the_past_year_have_there_been_times_when_you_felt_that_a')
        THEN 'REDACTED'
        ELSE in_the_past_year_have_there_been_times_when_you_felt_that_a::text
    END as in_the_past_year_have_there_been_times_when_you_felt_that_a,
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
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-id-mw-tool-ipc-s4-pdf')
        THEN 'REDACTED'
        ELSE "task-id-mw-tool-ipc-s4-pdf"::text
    END as "task-id-mw-tool-ipc-s4-pdf",
        CURRENT_TIMESTAMP as anonymized_at

from source_data