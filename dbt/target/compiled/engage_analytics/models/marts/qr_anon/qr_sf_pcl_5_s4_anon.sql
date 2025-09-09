

-- Anonymized view for qr_sf_pcl_5_s4 with PII fields masked based on questionnaire_metadata.anon flag
-- Questionnaire: Short-Form PCL-5-8 (IPC Session 4) (Questionnaire/210)
-- PII fields masked: 2 fields

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
        ipc_s4_encounter_id_of_ptsd_session_4,
        ipc_s4_in_the_past_month_how_much_were_you_bothered_byfee,
        ipc_s4_in_the_past_month_how_much_were_you_bothered_by_re,
        ipc_s4_in_the_past_month_how_much_were_you_bothered_bylos,
        ipc_s4_total_score,
        'REDACTED' as ipc_s4_in_the_past_month_how_much_were_you_bothered_by_ha,
        ipc_s4_in_the_past_month_how_much_were_you_bothered_by_av,
        ipc_s4_in_the_past_month_how_much_were_you_bothered_by_fe,
        ipc_s4_in_the_past_month_how_much_were_you_bothered_by_av_9,
        'REDACTED' as ipc_s4_in_the_past_month_how_much_were_you_bothered_byhav,
        ipc_s4_score_meaning,
        ipc_s4_score_meaning_12,
        sometimes_things_happen_to_people_that_are_unusually_or_espe,
        i_am_going_to_list_some_of_these_events_please_tell_me_if_y,
        likely_has_ptsd,
        "f1.26.1",
        "f1.27.1",
        "f1.28.1",
        "f1.29.1",
        "f1.30.1",
        CURRENT_TIMESTAMP as anonymized_at

from "airbyte"."engage_analytics_engage_analytics_mart"."qr_sf_pcl_5_s4"