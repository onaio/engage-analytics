

-- Anonymized view for qr_sf_pcl_5_s1 with PII fields masked based on questionnaire_metadata.anon flag
-- Auto-generated to mask fields marked as anon=TRUE in questionnaire_metadata

with metadata_pii as (
    select distinct
        question_alias
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where questionnaire_id in ('Questionnaire/204')
    and anon = 'TRUE'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_sf_pcl_5_s1"
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
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_taskid_ptsd_pdf')
        THEN 'REDACTED'
        ELSE ipc_s1_taskid_ptsd_pdf::text
    END as ipc_s1_taskid_ptsd_pdf,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_in_the_past_month_how_much_were_you_bothered_byfee')
        THEN 'REDACTED'
        ELSE ipc_s1_in_the_past_month_how_much_were_you_bothered_byfee::text
    END as ipc_s1_in_the_past_month_how_much_were_you_bothered_byfee,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_in_the_past_month_how_much_were_you_bothered_by_re')
        THEN 'REDACTED'
        ELSE ipc_s1_in_the_past_month_how_much_were_you_bothered_by_re::text
    END as ipc_s1_in_the_past_month_how_much_were_you_bothered_by_re,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_in_the_past_month_how_much_were_you_bothered_bylos')
        THEN 'REDACTED'
        ELSE ipc_s1_in_the_past_month_how_much_were_you_bothered_bylos::text
    END as ipc_s1_in_the_past_month_how_much_were_you_bothered_bylos,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_in_the_past_month_how_much_were_you_bothered_by_ha')
        THEN 'REDACTED'
        ELSE ipc_s1_in_the_past_month_how_much_were_you_bothered_by_ha::text
    END as ipc_s1_in_the_past_month_how_much_were_you_bothered_by_ha,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_in_the_past_month_how_much_were_you_bothered_by_av')
        THEN 'REDACTED'
        ELSE ipc_s1_in_the_past_month_how_much_were_you_bothered_by_av::text
    END as ipc_s1_in_the_past_month_how_much_were_you_bothered_by_av,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_in_the_past_month_how_much_were_you_bothered_by_fe')
        THEN 'REDACTED'
        ELSE ipc_s1_in_the_past_month_how_much_were_you_bothered_by_fe::text
    END as ipc_s1_in_the_past_month_how_much_were_you_bothered_by_fe,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_in_the_past_month_how_much_were_you_bothered_by_av_8')
        THEN 'REDACTED'
        ELSE ipc_s1_in_the_past_month_how_much_were_you_bothered_by_av_8::text
    END as ipc_s1_in_the_past_month_how_much_were_you_bothered_by_av_8,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_in_the_past_month_how_much_were_you_bothered_byhav')
        THEN 'REDACTED'
        ELSE ipc_s1_in_the_past_month_how_much_were_you_bothered_byhav::text
    END as ipc_s1_in_the_past_month_how_much_were_you_bothered_byhav,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_encounter_id_of_ptsd_session_1')
        THEN 'REDACTED'
        ELSE ipc_s1_encounter_id_of_ptsd_session_1::text
    END as ipc_s1_encounter_id_of_ptsd_session_1,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_observation_id_of_pcptsd_session_1_score')
        THEN 'REDACTED'
        ELSE ipc_s1_observation_id_of_pcptsd_session_1_score::text
    END as ipc_s1_observation_id_of_pcptsd_session_1_score,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_total_score')
        THEN 'REDACTED'
        ELSE ipc_s1_total_score::text
    END as ipc_s1_total_score,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_score_meaning')
        THEN 'REDACTED'
        ELSE ipc_s1_score_meaning::text
    END as ipc_s1_score_meaning,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_score_meaning_14')
        THEN 'REDACTED'
        ELSE ipc_s1_score_meaning_14::text
    END as ipc_s1_score_meaning_14,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_is_ptsd')
        THEN 'REDACTED'
        ELSE ipc_s1_is_ptsd::text
    END as ipc_s1_is_ptsd,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'date_of_birth')
        THEN 'REDACTED'
        ELSE date_of_birth::text
    END as date_of_birth,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'age')
        THEN 'REDACTED'
        ELSE age::text
    END as age,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'birth_month')
        THEN 'REDACTED'
        ELSE birth_month::text
    END as birth_month,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'age_years')
        THEN 'REDACTED'
        ELSE age_years::text
    END as age_years,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'encounter_reference')
        THEN 'REDACTED'
        ELSE encounter_reference::text
    END as encounter_reference,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sometimes_things_happen_to_people_that_are_unusually_or_espe')
        THEN 'REDACTED'
        ELSE sometimes_things_happen_to_people_that_are_unusually_or_espe::text
    END as sometimes_things_happen_to_people_that_are_unusually_or_espe,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'likely_has_ptsd')
        THEN 'REDACTED'
        ELSE likely_has_ptsd::text
    END as likely_has_ptsd,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'i_am_going_to_list_some_of_these_events_please_tell_me_if_y')
        THEN 'REDACTED'
        ELSE i_am_going_to_list_some_of_these_events_please_tell_me_if_y::text
    END as i_am_going_to_list_some_of_these_events_please_tell_me_if_y,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'f1.26.1')
        THEN 'REDACTED'
        ELSE "f1.26.1"::text
    END as "f1.26.1",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'f1.27.1')
        THEN 'REDACTED'
        ELSE "f1.27.1"::text
    END as "f1.27.1",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'f1.28.1')
        THEN 'REDACTED'
        ELSE "f1.28.1"::text
    END as "f1.28.1",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'f1.29.1')
        THEN 'REDACTED'
        ELSE "f1.29.1"::text
    END as "f1.29.1",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'f1.30.1')
        THEN 'REDACTED'
        ELSE "f1.30.1"::text
    END as "f1.30.1",
        CURRENT_TIMESTAMP as anonymized_at

from source_data