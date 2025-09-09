{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_start_ipc_s4 with PII fields masked based on questionnaire_metadata.anon flag
-- Questionnaire: IPC Session 4 (Questionnaire/63)
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
        ipc_s4_phq9_total,
        ipc_s4_gad7_total,
        ipc_s4_ptsd_total,
        ipc_s4_taskid_session_4_navigator,
        ipc_s4_taskid_ipc_session_4_provider_pdf,
        ipc_s4_in_what_format_did_you_deliver_this_session_with_t,
        ipc_s4_taskid_ipc_session_4_client_pdf,
        ipc_s4_what_we_know_is_that_symptoms_can_come_back_in_the,
        'REDACTED' as ipc_s4_did_you_complete_the_required_assessments_and_the_,
        ipc_s4_after_todays_session_i_can_refer_you_for_clinical_,
        'REDACTED' as ipc_s4_after_todays_session_i_will_be_referring_you_for_a,
        ipc_s4_do_you_remember_why_you_came_lets_talk_about_how_y,
        ipc_s4_what_do_you_think_are_some_situations_that_may_tri,
        ipc_s4_what_do_you_think_are_some_situations_that_may_tri_14,
        ipc_s4_how_does_it_make_you_feel_to_know_that_today_is_ou,
        ipc_s4_lets_look_at_your_symptom_changes_what_were_your_s,
        ipc_s4_how_do_you_feel_about_today_being_our_last_session,
        ipc_s4_how_would_you_know_if_your_symptoms_are_coming_bac,
        ipc_s4_what_are_some_of_the_skills_you_learned_here_that_,
        ipc_s4_please_select_how_you_would_like_to_proceed_with_t,
        ipc_s4_what_are_some_of_the_skills_you_learned_here_that__21,
        ipc_s4_lets_look_at_your_symptom_changes_what_were_your_s_22,
        ipc_s4_do_you_remember_why_you_came_lets_talk_about_how_y_23,
        ipc_s4_on_a_scale_of_1_to_10_with_1_being_the_worst_mood_,
        'REDACTED' as ipc_s4_mood_rating_total,
        ipc_s4_i_am_going_to_provide_you_with_our_agencys_contact,
        ipc_s4_what_do_you_think_you_still_need_to_work_on_regard,
        ipc_s4_what_do_you_think_you_still_need_to_work_on_regard_28,
        ipc_s4_lets_talk_about_what_skills_you_learned_here_that_,
        ipc_s4_lets_talk_about_what_skills_you_learned_here_that__30,
        ipc_s4_7fef53bc56db47ce8cba8efd4bcbc318,
        ipc_s4_total_score,
        did_you_discuss_this_clients_severe_mental_health_screening,
        'REDACTED' as what_is_the_recommended_plan_to_address_the_probable_severe,
        feeling_changed,
        referral_details_if_the_client_is_referred_out,
        rerer,
        "20241031",
        "20250201",
        supervisors_name,
        "is-connected-to-clinical-care-details",
        "is-connected-to-clinical-care-question",
        CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_start_ipc_s4') }}