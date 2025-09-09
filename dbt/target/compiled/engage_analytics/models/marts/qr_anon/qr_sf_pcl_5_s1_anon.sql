

-- Anonymized view for qr_sf_pcl_5_s1 with PII fields masked based on questionnaire_metadata.anon flag
-- Questionnaire: Short-Form PCL-5-8 (IPC Session 1) (Questionnaire/204)
-- PII fields masked: 3 fields

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
        ipc_s1_taskid_ptsd_pdf,
        ipc_s1_in_the_past_month_how_much_were_you_bothered_byfee,
        ipc_s1_in_the_past_month_how_much_were_you_bothered_by_re,
        ipc_s1_in_the_past_month_how_much_were_you_bothered_bylos,
        'REDACTED' as ipc_s1_in_the_past_month_how_much_were_you_bothered_by_ha,
        ipc_s1_in_the_past_month_how_much_were_you_bothered_by_av,
        ipc_s1_in_the_past_month_how_much_were_you_bothered_by_fe,
        ipc_s1_in_the_past_month_how_much_were_you_bothered_by_av_8,
        'REDACTED' as ipc_s1_in_the_past_month_how_much_were_you_bothered_byhav,
        ipc_s1_encounter_id_of_ptsd_session_1,
        ipc_s1_observation_id_of_pcptsd_session_1_score,
        ipc_s1_total_score,
        ipc_s1_score_meaning,
        ipc_s1_score_meaning_14,
        ipc_s1_is_ptsd,
        'REDACTED' as date_of_birth,
        age,
        birth_month,
        age_years,
        encounter_reference,
        sometimes_things_happen_to_people_that_are_unusually_or_espe,
        likely_has_ptsd,
        i_am_going_to_list_some_of_these_events_please_tell_me_if_y,
        "f1.26.1",
        "f1.27.1",
        "f1.28.1",
        "f1.29.1",
        "f1.30.1",
        CURRENT_TIMESTAMP as anonymized_at

from "airbyte"."engage_analytics_engage_analytics_mart"."qr_sf_pcl_5_s1"