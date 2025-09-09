{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_mw_tool_ipc_session_4 with PII fields masked based on questionnaire_metadata.anon flag
-- Questionnaire: Mw Tool Ipc Session 4 (Questionnaire/mw-tool-ipc-session-4)
-- PII fields masked: 6 fields

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
        in_the_past_year_have_there_been_times_when_you_felt_that_s,
        in_the_past_year_have_you_ever_felt_that_your_thoughts_were,
        in_the_past_year_how_often_do_you_have_a_drink_containing_a,
        in_the_past_month_have_you_wished_you_were_dead_or_wished_y,
        in_the_past_year_did_you_at_any_time_hear_voices_saying_qui,
        in_the_last_2_weeks_how_often_have_you_been_feeling_down_d,
        in_the_past_year_how_many_times_have_you_used_a_recreationa,
        in_the_past_year_how_many_drinks_containing_alcohol_do_you,
        in_the_past_3_months_have_you_ever_done_anything_started_t,
        in_the_last_2_weeks_how_often_have_you_been_feeling_nervous,
        in_the_past_month_have_you_had_any_actual_thoughts_of_killi,
        in_the_last_2_weeks_how_often_have_you_been_so_restless_tha,
        'REDACTED' as in_the_past_year_have_there_been_times_when_you_felt_that_a,
        "alcohol-problem",
        "common-mental-health",
        "drug-problem",
        "how-many-drinks",
        "no-mental-problem",
        'REDACTED' as "patient-age",
        'REDACTED' as "patient-biological-sex",
        'REDACTED' as "patient-dob",
        'REDACTED' as "patient-gender-identity",
        "patient-how-you-think-of-yourself",
        'REDACTED' as "patient-name",
        "patient-pronouns",
        "severe-mental-health",
        "suicide-risk",
        "task-id-mw-tool-ipc-s4-pdf",
        CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_mw_tool_ipc_session_4') }}