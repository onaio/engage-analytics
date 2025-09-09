{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_start_ipc_s1 with PII fields masked based on questionnaire_metadata.anon flag
-- Auto-generated to mask fields marked as anon=TRUE in questionnaire_metadata

with metadata_pii as (
    select distinct
        question_alias
    from {{ ref('questionnaire_metadata') }}
    where questionnaire_id in ('Questionnaire/55')
    and anon = 'TRUE'
),

source_data as (
    select * from {{ ref('qr_start_ipc_s1') }}
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
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_patient_age')
        THEN 'REDACTED'
        ELSE ipc_s1_patient_age::text
    END as ipc_s1_patient_age,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_patient_dob')
        THEN 'REDACTED'
        ELSE ipc_s1_patient_dob::text
    END as ipc_s1_patient_dob,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_in_what_format_did_you_deliver_this_session_with_t')
        THEN 'REDACTED'
        ELSE ipc_s1_in_what_format_did_you_deliver_this_session_with_t::text
    END as ipc_s1_in_what_format_did_you_deliver_this_session_with_t,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_in_the_past_6_months')
        THEN 'REDACTED'
        ELSE ipc_s1_in_the_past_6_months::text
    END as ipc_s1_in_the_past_6_months,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_f1921')
        THEN 'REDACTED'
        ELSE ipc_s1_f1921::text
    END as ipc_s1_f1921,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_in_the_past_1_year')
        THEN 'REDACTED'
        ELSE ipc_s1_in_the_past_1_year::text
    END as ipc_s1_in_the_past_1_year,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_i_noticed_that_when_i_was_asking_about_important_p')
        THEN 'REDACTED'
        ELSE ipc_s1_i_noticed_that_when_i_was_asking_about_important_p::text
    END as ipc_s1_i_noticed_that_when_i_was_asking_about_important_p,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_did_you_complete_the_required_assessments_and_the_')
        THEN 'REDACTED'
        ELSE ipc_s1_did_you_complete_the_required_assessments_and_the_::text
    END as ipc_s1_did_you_complete_the_required_assessments_and_the_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_do_you_think_an_important_persons_death_is_connect')
        THEN 'REDACTED'
        ELSE ipc_s1_do_you_think_an_important_persons_death_is_connect::text
    END as ipc_s1_do_you_think_an_important_persons_death_is_connect,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_are_you_and_someone_else_having_a_strong_disagreem')
        THEN 'REDACTED'
        ELSE ipc_s1_are_you_and_someone_else_having_a_strong_disagreem::text
    END as ipc_s1_are_you_and_someone_else_having_a_strong_disagreem,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_briefly_tell_me_about_the_disagreement')
        THEN 'REDACTED'
        ELSE ipc_s1_briefly_tell_me_about_the_disagreement::text
    END as ipc_s1_briefly_tell_me_about_the_disagreement,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_how_has_this_disagreement_been_affecting_you')
        THEN 'REDACTED'
        ELSE ipc_s1_how_has_this_disagreement_been_affecting_you::text
    END as ipc_s1_how_has_this_disagreement_been_affecting_you,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_have_there_been_any_changes_in_your_life_that_are_')
        THEN 'REDACTED'
        ELSE ipc_s1_have_there_been_any_changes_in_your_life_that_are_::text
    END as ipc_s1_have_there_been_any_changes_in_your_life_that_are_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_what_was_the_change')
        THEN 'REDACTED'
        ELSE ipc_s1_what_was_the_change::text
    END as ipc_s1_what_was_the_change,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_when_did_this_happen')
        THEN 'REDACTED'
        ELSE ipc_s1_when_did_this_happen::text
    END as ipc_s1_when_did_this_happen,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_if_the_client_did_not_mention_any_close_relationsh')
        THEN 'REDACTED'
        ELSE ipc_s1_if_the_client_did_not_mention_any_close_relationsh::text
    END as ipc_s1_if_the_client_did_not_mention_any_close_relationsh,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_would_you_say_you_have_close_relationships')
        THEN 'REDACTED'
        ELSE ipc_s1_would_you_say_you_have_close_relationships::text
    END as ipc_s1_would_you_say_you_have_close_relationships,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_has_anything_changed_in_your_life_so_that_now_you_')
        THEN 'REDACTED'
        ELSE ipc_s1_has_anything_changed_in_your_life_so_that_now_you_::text
    END as ipc_s1_has_anything_changed_in_your_life_so_that_now_you_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_f1882')
        THEN 'REDACTED'
        ELSE ipc_s1_f1882::text
    END as ipc_s1_f1882,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_select_the_possible_interpersonal_problem_areas_yo')
        THEN 'REDACTED'
        ELSE ipc_s1_select_the_possible_interpersonal_problem_areas_yo::text
    END as ipc_s1_select_the_possible_interpersonal_problem_areas_yo,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_your_most_distressing_current_symptoms_began')
        THEN 'REDACTED'
        ELSE ipc_s1_your_most_distressing_current_symptoms_began::text
    END as ipc_s1_your_most_distressing_current_symptoms_began,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_taskid_session_1_navigator')
        THEN 'REDACTED'
        ELSE ipc_s1_taskid_session_1_navigator::text
    END as ipc_s1_taskid_session_1_navigator,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_f1402')
        THEN 'REDACTED'
        ELSE ipc_s1_f1402::text
    END as ipc_s1_f1402,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_when_did_this_event_happen')
        THEN 'REDACTED'
        ELSE ipc_s1_when_did_this_event_happen::text
    END as ipc_s1_when_did_this_event_happen,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_is_there_anything_else_that_we_did_not_discuss_tha')
        THEN 'REDACTED'
        ELSE ipc_s1_is_there_anything_else_that_we_did_not_discuss_tha::text
    END as ipc_s1_is_there_anything_else_that_we_did_not_discuss_tha,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_taskid_session_2_navigator')
        THEN 'REDACTED'
        ELSE ipc_s1_taskid_session_2_navigator::text
    END as ipc_s1_taskid_session_2_navigator,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_on_a_scale_of_1_to_10_with_1_being_the_worst_mood_')
        THEN 'REDACTED'
        ELSE ipc_s1_on_a_scale_of_1_to_10_with_1_being_the_worst_mood_::text
    END as ipc_s1_on_a_scale_of_1_to_10_with_1_being_the_worst_mood_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_has_this_person_been_consistently_close_to_you_or_')
        THEN 'REDACTED'
        ELSE ipc_s1_has_this_person_been_consistently_close_to_you_or_::text
    END as ipc_s1_has_this_person_been_consistently_close_to_you_or_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_tell_me_about_your_relationship_with_this_person_h')
        THEN 'REDACTED'
        ELSE ipc_s1_tell_me_about_your_relationship_with_this_person_h::text
    END as ipc_s1_tell_me_about_your_relationship_with_this_person_h,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_when_did_this_disagreement_happen')
        THEN 'REDACTED'
        ELSE ipc_s1_when_did_this_disagreement_happen::text
    END as ipc_s1_when_did_this_disagreement_happen,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_do_you_want_to_propose_an_interpersonal_formulatio')
        THEN 'REDACTED'
        ELSE ipc_s1_do_you_want_to_propose_an_interpersonal_formulatio::text
    END as ipc_s1_do_you_want_to_propose_an_interpersonal_formulatio,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_do_you_agree_with_this')
        THEN 'REDACTED'
        ELSE ipc_s1_do_you_agree_with_this::text
    END as ipc_s1_do_you_agree_with_this,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_these_are_the_symptoms_you_said_you_are_currently_')
        THEN 'REDACTED'
        ELSE ipc_s1_these_are_the_symptoms_you_said_you_are_currently_::text
    END as ipc_s1_these_are_the_symptoms_you_said_you_are_currently_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_taskid_mood_pdf')
        THEN 'REDACTED'
        ELSE ipc_s1_taskid_mood_pdf::text
    END as ipc_s1_taskid_mood_pdf,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_relationships_eg_talk_with_friends_or_vi')
        THEN 'REDACTED'
        ELSE ipc_s1_relationships_eg_talk_with_friends_or_vi::text
    END as ipc_s1_relationships_eg_talk_with_friends_or_vi,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_is_there_anything_that_could_get_in_the_way_of_you')
        THEN 'REDACTED'
        ELSE ipc_s1_is_there_anything_that_could_get_in_the_way_of_you::text
    END as ipc_s1_is_there_anything_that_could_get_in_the_way_of_you,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_f1813')
        THEN 'REDACTED'
        ELSE ipc_s1_f1813::text
    END as ipc_s1_f1813,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_at_that_time_you_were_experiencing')
        THEN 'REDACTED'
        ELSE ipc_s1_at_that_time_you_were_experiencing::text
    END as ipc_s1_at_that_time_you_were_experiencing,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_taskid_ipc_session_1_provider_pdf')
        THEN 'REDACTED'
        ELSE ipc_s1_taskid_ipc_session_1_provider_pdf::text
    END as ipc_s1_taskid_ipc_session_1_provider_pdf,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_work_on_my_relationship_with_my_parents_')
        THEN 'REDACTED'
        ELSE ipc_s1_work_on_my_relationship_with_my_parents_::text
    END as ipc_s1_work_on_my_relationship_with_my_parents_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_f13931')
        THEN 'REDACTED'
        ELSE ipc_s1_f13931::text
    END as ipc_s1_f13931,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_taskid_ipc_session_1_client_pdf')
        THEN 'REDACTED'
        ELSE ipc_s1_taskid_ipc_session_1_client_pdf::text
    END as ipc_s1_taskid_ipc_session_1_client_pdf,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_of_these_symptoms_which_are_the_most_distressing')
        THEN 'REDACTED'
        ELSE ipc_s1_of_these_symptoms_which_are_the_most_distressing::text
    END as ipc_s1_of_these_symptoms_which_are_the_most_distressing,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_f1814')
        THEN 'REDACTED'
        ELSE ipc_s1_f1814::text
    END as ipc_s1_f1814,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_earlier_than_that_you_noticed_symptoms')
        THEN 'REDACTED'
        ELSE ipc_s1_earlier_than_that_you_noticed_symptoms::text
    END as ipc_s1_earlier_than_that_you_noticed_symptoms,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_community_activities_eg_go_to_a_recreati')
        THEN 'REDACTED'
        ELSE ipc_s1_community_activities_eg_go_to_a_recreati::text
    END as ipc_s1_community_activities_eg_go_to_a_recreati,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_patient_name')
        THEN 'REDACTED'
        ELSE ipc_s1_patient_name::text
    END as ipc_s1_patient_name,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_f13932')
        THEN 'REDACTED'
        ELSE ipc_s1_f13932::text
    END as ipc_s1_f13932,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_f1405')
        THEN 'REDACTED'
        ELSE ipc_s1_f1405::text
    END as ipc_s1_f1405,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_when_did_these_symptoms_begin')
        THEN 'REDACTED'
        ELSE ipc_s1_when_did_these_symptoms_begin::text
    END as ipc_s1_when_did_these_symptoms_begin,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_xxx')
        THEN 'REDACTED'
        ELSE ipc_s1_xxx::text
    END as ipc_s1_xxx,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_yesterday_')
        THEN 'REDACTED'
        ELSE ipc_s1_yesterday_::text
    END as ipc_s1_yesterday_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_patient_gender')
        THEN 'REDACTED'
        ELSE ipc_s1_patient_gender::text
    END as ipc_s1_patient_gender,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_has_this_person_been_consistently_close_to_you_or__54')
        THEN 'REDACTED'
        ELSE ipc_s1_has_this_person_been_consistently_close_to_you_or__54::text
    END as ipc_s1_has_this_person_been_consistently_close_to_you_or__54,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_phq9_total')
        THEN 'REDACTED'
        ELSE ipc_s1_phq9_total::text
    END as ipc_s1_phq9_total,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_possible_problem_areas')
        THEN 'REDACTED'
        ELSE ipc_s1_possible_problem_areas::text
    END as ipc_s1_possible_problem_areas,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_at_that_time_you_were_experiencing_57')
        THEN 'REDACTED'
        ELSE ipc_s1_at_that_time_you_were_experiencing_57::text
    END as ipc_s1_at_that_time_you_were_experiencing_57,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_gad7_total')
        THEN 'REDACTED'
        ELSE ipc_s1_gad7_total::text
    END as ipc_s1_gad7_total,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_hobbies_eg_listen_to_music_play_video_ga')
        THEN 'REDACTED'
        ELSE ipc_s1_hobbies_eg_listen_to_music_play_video_ga::text
    END as ipc_s1_hobbies_eg_listen_to_music_play_video_ga,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_what_was_going_on_in_your_life_when_you_started_to')
        THEN 'REDACTED'
        ELSE ipc_s1_what_was_going_on_in_your_life_when_you_started_to::text
    END as ipc_s1_what_was_going_on_in_your_life_when_you_started_to,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_total_score')
        THEN 'REDACTED'
        ELSE ipc_s1_total_score::text
    END as ipc_s1_total_score,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_ptsd_total')
        THEN 'REDACTED'
        ELSE ipc_s1_ptsd_total::text
    END as ipc_s1_ptsd_total,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_zfgh')
        THEN 'REDACTED'
        ELSE ipc_s1_zfgh::text
    END as ipc_s1_zfgh,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_how_has_this_relationship_changed_following_trauma')
        THEN 'REDACTED'
        ELSE ipc_s1_how_has_this_relationship_changed_following_trauma::text
    END as ipc_s1_how_has_this_relationship_changed_following_trauma,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_do_you_agree_with_this_summary')
        THEN 'REDACTED'
        ELSE ipc_s1_do_you_agree_with_this_summary::text
    END as ipc_s1_do_you_agree_with_this_summary,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_f1408')
        THEN 'REDACTED'
        ELSE ipc_s1_f1408::text
    END as ipc_s1_f1408,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_observation_id_of_mood_rating_session_1_score')
        THEN 'REDACTED'
        ELSE ipc_s1_observation_id_of_mood_rating_session_1_score::text
    END as ipc_s1_observation_id_of_mood_rating_session_1_score,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_what_about_before_that_was_there_a_time_before_in_')
        THEN 'REDACTED'
        ELSE ipc_s1_what_about_before_that_was_there_a_time_before_in_::text
    END as ipc_s1_what_about_before_that_was_there_a_time_before_in_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_religionspirituality_eg_pray_or_go_to_se')
        THEN 'REDACTED'
        ELSE ipc_s1_religionspirituality_eg_pray_or_go_to_se::text
    END as ipc_s1_religionspirituality_eg_pray_or_go_to_se,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_mood_rating_total')
        THEN 'REDACTED'
        ELSE ipc_s1_mood_rating_total::text
    END as ipc_s1_mood_rating_total,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_person_1_is')
        THEN 'REDACTED'
        ELSE ipc_s1_person_1_is::text
    END as ipc_s1_person_1_is,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_has_this_person_been_consistently_close_to_you_or__72')
        THEN 'REDACTED'
        ELSE ipc_s1_has_this_person_been_consistently_close_to_you_or__72::text
    END as ipc_s1_has_this_person_been_consistently_close_to_you_or__72,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_phq9_total_meaning')
        THEN 'REDACTED'
        ELSE ipc_s1_phq9_total_meaning::text
    END as ipc_s1_phq9_total_meaning,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_what_were_the_symptoms')
        THEN 'REDACTED'
        ELSE ipc_s1_what_were_the_symptoms::text
    END as ipc_s1_what_were_the_symptoms,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_with_my_pastor_')
        THEN 'REDACTED'
        ELSE ipc_s1_with_my_pastor_::text
    END as ipc_s1_with_my_pastor_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_gad7_total_meaning')
        THEN 'REDACTED'
        ELSE ipc_s1_gad7_total_meaning::text
    END as ipc_s1_gad7_total_meaning,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_f13941')
        THEN 'REDACTED'
        ELSE ipc_s1_f13941::text
    END as ipc_s1_f13941,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_ptsd_total_meaning_toggle')
        THEN 'REDACTED'
        ELSE ipc_s1_ptsd_total_meaning_toggle::text
    END as ipc_s1_ptsd_total_meaning_toggle,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_when_did_you_notice_these_symptoms')
        THEN 'REDACTED'
        ELSE ipc_s1_when_did_you_notice_these_symptoms::text
    END as ipc_s1_when_did_you_notice_these_symptoms,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_other')
        THEN 'REDACTED'
        ELSE ipc_s1_other::text
    END as ipc_s1_other,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_mood_rating_comment')
        THEN 'REDACTED'
        ELSE ipc_s1_mood_rating_comment::text
    END as ipc_s1_mood_rating_comment,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_f1384')
        THEN 'REDACTED'
        ELSE ipc_s1_f1384::text
    END as ipc_s1_f1384,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_f13942')
        THEN 'REDACTED'
        ELSE ipc_s1_f13942::text
    END as ipc_s1_f13942,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_f14011')
        THEN 'REDACTED'
        ELSE ipc_s1_f14011::text
    END as ipc_s1_f14011,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_what_was_happening_then')
        THEN 'REDACTED'
        ELSE ipc_s1_what_was_happening_then::text
    END as ipc_s1_what_was_happening_then,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_encounter_id_of_ipc_session_1')
        THEN 'REDACTED'
        ELSE ipc_s1_encounter_id_of_ipc_session_1::text
    END as ipc_s1_encounter_id_of_ipc_session_1,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_has_this_person_been_consistently_close_to_you_or__87')
        THEN 'REDACTED'
        ELSE ipc_s1_has_this_person_been_consistently_close_to_you_or__87::text
    END as ipc_s1_has_this_person_been_consistently_close_to_you_or__87,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_ex')
        THEN 'REDACTED'
        ELSE ipc_s1_ex::text
    END as ipc_s1_ex,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_how_has_this_relationship_changed_following_trauma_89')
        THEN 'REDACTED'
        ELSE ipc_s1_how_has_this_relationship_changed_following_trauma_89::text
    END as ipc_s1_how_has_this_relationship_changed_following_trauma_89,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_person_2_is')
        THEN 'REDACTED'
        ELSE ipc_s1_person_2_is::text
    END as ipc_s1_person_2_is,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_f13951')
        THEN 'REDACTED'
        ELSE ipc_s1_f13951::text
    END as ipc_s1_f13951,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_f13952')
        THEN 'REDACTED'
        ELSE ipc_s1_f13952::text
    END as ipc_s1_f13952,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_gud')
        THEN 'REDACTED'
        ELSE ipc_s1_gud::text
    END as ipc_s1_gud,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_how_has_this_relationship_changed_following_trauma_94')
        THEN 'REDACTED'
        ELSE ipc_s1_how_has_this_relationship_changed_following_trauma_94::text
    END as ipc_s1_how_has_this_relationship_changed_following_trauma_94,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_person_3_is')
        THEN 'REDACTED'
        ELSE ipc_s1_person_3_is::text
    END as ipc_s1_person_3_is,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_f13961')
        THEN 'REDACTED'
        ELSE ipc_s1_f13961::text
    END as ipc_s1_f13961,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_f13962')
        THEN 'REDACTED'
        ELSE ipc_s1_f13962::text
    END as ipc_s1_f13962,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_how_has_this_relationship_changed_following_trauma_98')
        THEN 'REDACTED'
        ELSE ipc_s1_how_has_this_relationship_changed_following_trauma_98::text
    END as ipc_s1_how_has_this_relationship_changed_following_trauma_98,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_person_4_is')
        THEN 'REDACTED'
        ELSE ipc_s1_person_4_is::text
    END as ipc_s1_person_4_is,
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
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'seeing_someone_be_killed_or_seriously_injured')
        THEN 'REDACTED'
        ELSE seeing_someone_be_killed_or_seriously_injured::text
    END as seeing_someone_be_killed_or_seriously_injured,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'f1.38.3')
        THEN 'REDACTED'
        ELSE "f1.38.3"::text
    END as "f1.38.3",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'f1.46.2')
        THEN 'REDACTED'
        ELSE "f1.46.2"::text
    END as "f1.46.2",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-id-008')
        THEN 'REDACTED'
        ELSE "task-id-008"::text
    END as "task-id-008",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-id-009')
        THEN 'REDACTED'
        ELSE "task-id-009"::text
    END as "task-id-009",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-id-010')
        THEN 'REDACTED'
        ELSE "task-id-010"::text
    END as "task-id-010",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-id-012')
        THEN 'REDACTED'
        ELSE "task-id-012"::text
    END as "task-id-012",
        CURRENT_TIMESTAMP as anonymized_at

from source_data