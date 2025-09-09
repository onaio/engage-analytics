

-- Anonymized view for qr_start_ipc_s4 with PII fields masked based on questionnaire_metadata.anon flag
-- Auto-generated to mask fields marked as anon=TRUE in questionnaire_metadata

with metadata_pii as (
    select distinct
        question_alias
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where questionnaire_id in ('Questionnaire/63')
    and anon = 'TRUE'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_start_ipc_s4"
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
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_phq9_total')
        THEN 'REDACTED'
        ELSE ipc_s4_phq9_total::text
    END as ipc_s4_phq9_total,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_gad7_total')
        THEN 'REDACTED'
        ELSE ipc_s4_gad7_total::text
    END as ipc_s4_gad7_total,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_ptsd_total')
        THEN 'REDACTED'
        ELSE ipc_s4_ptsd_total::text
    END as ipc_s4_ptsd_total,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_taskid_session_4_navigator')
        THEN 'REDACTED'
        ELSE ipc_s4_taskid_session_4_navigator::text
    END as ipc_s4_taskid_session_4_navigator,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_taskid_ipc_session_4_provider_pdf')
        THEN 'REDACTED'
        ELSE ipc_s4_taskid_ipc_session_4_provider_pdf::text
    END as ipc_s4_taskid_ipc_session_4_provider_pdf,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_in_what_format_did_you_deliver_this_session_with_t')
        THEN 'REDACTED'
        ELSE ipc_s4_in_what_format_did_you_deliver_this_session_with_t::text
    END as ipc_s4_in_what_format_did_you_deliver_this_session_with_t,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_taskid_ipc_session_4_client_pdf')
        THEN 'REDACTED'
        ELSE ipc_s4_taskid_ipc_session_4_client_pdf::text
    END as ipc_s4_taskid_ipc_session_4_client_pdf,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_what_we_know_is_that_symptoms_can_come_back_in_the')
        THEN 'REDACTED'
        ELSE ipc_s4_what_we_know_is_that_symptoms_can_come_back_in_the::text
    END as ipc_s4_what_we_know_is_that_symptoms_can_come_back_in_the,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_did_you_complete_the_required_assessments_and_the_')
        THEN 'REDACTED'
        ELSE ipc_s4_did_you_complete_the_required_assessments_and_the_::text
    END as ipc_s4_did_you_complete_the_required_assessments_and_the_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_after_todays_session_i_can_refer_you_for_clinical_')
        THEN 'REDACTED'
        ELSE ipc_s4_after_todays_session_i_can_refer_you_for_clinical_::text
    END as ipc_s4_after_todays_session_i_can_refer_you_for_clinical_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_after_todays_session_i_will_be_referring_you_for_a')
        THEN 'REDACTED'
        ELSE ipc_s4_after_todays_session_i_will_be_referring_you_for_a::text
    END as ipc_s4_after_todays_session_i_will_be_referring_you_for_a,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_do_you_remember_why_you_came_lets_talk_about_how_y')
        THEN 'REDACTED'
        ELSE ipc_s4_do_you_remember_why_you_came_lets_talk_about_how_y::text
    END as ipc_s4_do_you_remember_why_you_came_lets_talk_about_how_y,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_what_do_you_think_are_some_situations_that_may_tri')
        THEN 'REDACTED'
        ELSE ipc_s4_what_do_you_think_are_some_situations_that_may_tri::text
    END as ipc_s4_what_do_you_think_are_some_situations_that_may_tri,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_what_do_you_think_are_some_situations_that_may_tri_14')
        THEN 'REDACTED'
        ELSE ipc_s4_what_do_you_think_are_some_situations_that_may_tri_14::text
    END as ipc_s4_what_do_you_think_are_some_situations_that_may_tri_14,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_how_does_it_make_you_feel_to_know_that_today_is_ou')
        THEN 'REDACTED'
        ELSE ipc_s4_how_does_it_make_you_feel_to_know_that_today_is_ou::text
    END as ipc_s4_how_does_it_make_you_feel_to_know_that_today_is_ou,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_lets_look_at_your_symptom_changes_what_were_your_s')
        THEN 'REDACTED'
        ELSE ipc_s4_lets_look_at_your_symptom_changes_what_were_your_s::text
    END as ipc_s4_lets_look_at_your_symptom_changes_what_were_your_s,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_how_do_you_feel_about_today_being_our_last_session')
        THEN 'REDACTED'
        ELSE ipc_s4_how_do_you_feel_about_today_being_our_last_session::text
    END as ipc_s4_how_do_you_feel_about_today_being_our_last_session,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_how_would_you_know_if_your_symptoms_are_coming_bac')
        THEN 'REDACTED'
        ELSE ipc_s4_how_would_you_know_if_your_symptoms_are_coming_bac::text
    END as ipc_s4_how_would_you_know_if_your_symptoms_are_coming_bac,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_what_are_some_of_the_skills_you_learned_here_that_')
        THEN 'REDACTED'
        ELSE ipc_s4_what_are_some_of_the_skills_you_learned_here_that_::text
    END as ipc_s4_what_are_some_of_the_skills_you_learned_here_that_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_please_select_how_you_would_like_to_proceed_with_t')
        THEN 'REDACTED'
        ELSE ipc_s4_please_select_how_you_would_like_to_proceed_with_t::text
    END as ipc_s4_please_select_how_you_would_like_to_proceed_with_t,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_what_are_some_of_the_skills_you_learned_here_that__21')
        THEN 'REDACTED'
        ELSE ipc_s4_what_are_some_of_the_skills_you_learned_here_that__21::text
    END as ipc_s4_what_are_some_of_the_skills_you_learned_here_that__21,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_lets_look_at_your_symptom_changes_what_were_your_s_22')
        THEN 'REDACTED'
        ELSE ipc_s4_lets_look_at_your_symptom_changes_what_were_your_s_22::text
    END as ipc_s4_lets_look_at_your_symptom_changes_what_were_your_s_22,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_do_you_remember_why_you_came_lets_talk_about_how_y_23')
        THEN 'REDACTED'
        ELSE ipc_s4_do_you_remember_why_you_came_lets_talk_about_how_y_23::text
    END as ipc_s4_do_you_remember_why_you_came_lets_talk_about_how_y_23,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_on_a_scale_of_1_to_10_with_1_being_the_worst_mood_')
        THEN 'REDACTED'
        ELSE ipc_s4_on_a_scale_of_1_to_10_with_1_being_the_worst_mood_::text
    END as ipc_s4_on_a_scale_of_1_to_10_with_1_being_the_worst_mood_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_mood_rating_total')
        THEN 'REDACTED'
        ELSE ipc_s4_mood_rating_total::text
    END as ipc_s4_mood_rating_total,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_i_am_going_to_provide_you_with_our_agencys_contact')
        THEN 'REDACTED'
        ELSE ipc_s4_i_am_going_to_provide_you_with_our_agencys_contact::text
    END as ipc_s4_i_am_going_to_provide_you_with_our_agencys_contact,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_what_do_you_think_you_still_need_to_work_on_regard')
        THEN 'REDACTED'
        ELSE ipc_s4_what_do_you_think_you_still_need_to_work_on_regard::text
    END as ipc_s4_what_do_you_think_you_still_need_to_work_on_regard,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_what_do_you_think_you_still_need_to_work_on_regard_28')
        THEN 'REDACTED'
        ELSE ipc_s4_what_do_you_think_you_still_need_to_work_on_regard_28::text
    END as ipc_s4_what_do_you_think_you_still_need_to_work_on_regard_28,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_lets_talk_about_what_skills_you_learned_here_that_')
        THEN 'REDACTED'
        ELSE ipc_s4_lets_talk_about_what_skills_you_learned_here_that_::text
    END as ipc_s4_lets_talk_about_what_skills_you_learned_here_that_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_lets_talk_about_what_skills_you_learned_here_that__30')
        THEN 'REDACTED'
        ELSE ipc_s4_lets_talk_about_what_skills_you_learned_here_that__30::text
    END as ipc_s4_lets_talk_about_what_skills_you_learned_here_that__30,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_7fef53bc56db47ce8cba8efd4bcbc318')
        THEN 'REDACTED'
        ELSE ipc_s4_7fef53bc56db47ce8cba8efd4bcbc318::text
    END as ipc_s4_7fef53bc56db47ce8cba8efd4bcbc318,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s4_total_score')
        THEN 'REDACTED'
        ELSE ipc_s4_total_score::text
    END as ipc_s4_total_score,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'did_you_discuss_this_clients_severe_mental_health_screening')
        THEN 'REDACTED'
        ELSE did_you_discuss_this_clients_severe_mental_health_screening::text
    END as did_you_discuss_this_clients_severe_mental_health_screening,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'what_is_the_recommended_plan_to_address_the_probable_severe')
        THEN 'REDACTED'
        ELSE what_is_the_recommended_plan_to_address_the_probable_severe::text
    END as what_is_the_recommended_plan_to_address_the_probable_severe,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'feeling_changed')
        THEN 'REDACTED'
        ELSE feeling_changed::text
    END as feeling_changed,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'referral_details_if_the_client_is_referred_out')
        THEN 'REDACTED'
        ELSE referral_details_if_the_client_is_referred_out::text
    END as referral_details_if_the_client_is_referred_out,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'rerer')
        THEN 'REDACTED'
        ELSE rerer::text
    END as rerer,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = '20241031')
        THEN 'REDACTED'
        ELSE "20241031"::text
    END as "20241031",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = '20250201')
        THEN 'REDACTED'
        ELSE "20250201"::text
    END as "20250201",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'supervisors_name')
        THEN 'REDACTED'
        ELSE supervisors_name::text
    END as supervisors_name,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'is-connected-to-clinical-care-details')
        THEN 'REDACTED'
        ELSE "is-connected-to-clinical-care-details"::text
    END as "is-connected-to-clinical-care-details",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'is-connected-to-clinical-care-question')
        THEN 'REDACTED'
        ELSE "is-connected-to-clinical-care-question"::text
    END as "is-connected-to-clinical-care-question",
        CURRENT_TIMESTAMP as anonymized_at

from source_data