
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_start_ipc_s2_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_start_ipc_s2 with PII fields masked based on questionnaire_metadata.anon flag
-- Auto-generated to mask fields marked as anon=TRUE in questionnaire_metadata

with metadata_pii as (
    select distinct
        question_alias
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where questionnaire_id in ('Questionnaire/58')
    and anon = 'TRUE'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_start_ipc_s2"
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
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_phq9_total_meaning')
        THEN 'REDACTED'
        ELSE ipc_s2_phq9_total_meaning::text
    END as ipc_s2_phq9_total_meaning,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_gad7_total_meaning')
        THEN 'REDACTED'
        ELSE ipc_s2_gad7_total_meaning::text
    END as ipc_s2_gad7_total_meaning,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_ptsd_total')
        THEN 'REDACTED'
        ELSE ipc_s2_ptsd_total::text
    END as ipc_s2_ptsd_total,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_phq9_total')
        THEN 'REDACTED'
        ELSE ipc_s2_phq9_total::text
    END as ipc_s2_phq9_total,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_summary_when_1')
        THEN 'REDACTED'
        ELSE ipc_s2_summary_when_1::text
    END as ipc_s2_summary_when_1,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_most_distressing_symptoms')
        THEN 'REDACTED'
        ELSE ipc_s2_most_distressing_symptoms::text
    END as ipc_s2_most_distressing_symptoms,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_selected_problem_area')
        THEN 'REDACTED'
        ELSE ipc_s2_selected_problem_area::text
    END as ipc_s2_selected_problem_area,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_ptsd_total_meaning_toggle')
        THEN 'REDACTED'
        ELSE ipc_s2_ptsd_total_meaning_toggle::text
    END as ipc_s2_ptsd_total_meaning_toggle,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_gad7_total')
        THEN 'REDACTED'
        ELSE ipc_s2_gad7_total::text
    END as ipc_s2_gad7_total,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_taskid_session_2_navigator')
        THEN 'REDACTED'
        ELSE ipc_s2_taskid_session_2_navigator::text
    END as ipc_s2_taskid_session_2_navigator,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_in_what_format_did_you_deliver_this_session_with_t')
        THEN 'REDACTED'
        ELSE ipc_s2_in_what_format_did_you_deliver_this_session_with_t::text
    END as ipc_s2_in_what_format_did_you_deliver_this_session_with_t,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_7ff4b5cb2c44434b90e985c61a858097')
        THEN 'REDACTED'
        ELSE ipc_s2_7ff4b5cb2c44434b90e985c61a858097::text
    END as ipc_s2_7ff4b5cb2c44434b90e985c61a858097,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_taskid_session_3_navigator')
        THEN 'REDACTED'
        ELSE ipc_s2_taskid_session_3_navigator::text
    END as ipc_s2_taskid_session_3_navigator,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_have_you_proposed_the_interpersonal_formulation_in')
        THEN 'REDACTED'
        ELSE ipc_s2_have_you_proposed_the_interpersonal_formulation_in::text
    END as ipc_s2_have_you_proposed_the_interpersonal_formulation_in,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_taskid_ipc_session_2_provider_pdf')
        THEN 'REDACTED'
        ELSE ipc_s2_taskid_ipc_session_2_provider_pdf::text
    END as ipc_s2_taskid_ipc_session_2_provider_pdf,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_work_at_home_you_assigned_to_the_client_in_this_se')
        THEN 'REDACTED'
        ELSE ipc_s2_work_at_home_you_assigned_to_the_client_in_this_se::text
    END as ipc_s2_work_at_home_you_assigned_to_the_client_in_this_se,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_a4a9cad504db44e59a9d0f1bf40ca840')
        THEN 'REDACTED'
        ELSE ipc_s2_a4a9cad504db44e59a9d0f1bf40ca840::text
    END as ipc_s2_a4a9cad504db44e59a9d0f1bf40ca840,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_did_you_complete_the_required_assessments_and_the_')
        THEN 'REDACTED'
        ELSE ipc_s2_did_you_complete_the_required_assessments_and_the_::text
    END as ipc_s2_did_you_complete_the_required_assessments_and_the_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_taskid_ipc_session_2_client_pdf')
        THEN 'REDACTED'
        ELSE ipc_s2_taskid_ipc_session_2_client_pdf::text
    END as ipc_s2_taskid_ipc_session_2_client_pdf,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_747188c84fc043ea92b49baa32567f73')
        THEN 'REDACTED'
        ELSE ipc_s2_747188c84fc043ea92b49baa32567f73::text
    END as ipc_s2_747188c84fc043ea92b49baa32567f73,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_would_you_prefer_to_focus_on_this_new_problem_area')
        THEN 'REDACTED'
        ELSE ipc_s2_would_you_prefer_to_focus_on_this_new_problem_area::text
    END as ipc_s2_would_you_prefer_to_focus_on_this_new_problem_area,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_did_the_person_ever_hit_kick_push_or_slap_you')
        THEN 'REDACTED'
        ELSE ipc_s2_did_the_person_ever_hit_kick_push_or_slap_you::text
    END as ipc_s2_did_the_person_ever_hit_kick_push_or_slap_you,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_do_you_agree_with_this')
        THEN 'REDACTED'
        ELSE ipc_s2_do_you_agree_with_this::text
    END as ipc_s2_do_you_agree_with_this,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_encounter_id_of_ipc_session_2')
        THEN 'REDACTED'
        ELSE ipc_s2_encounter_id_of_ipc_session_2::text
    END as ipc_s2_encounter_id_of_ipc_session_2,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_on_a_scale_of_1_to_10_with_1_being_the_worst_mood_')
        THEN 'REDACTED'
        ELSE ipc_s2_on_a_scale_of_1_to_10_with_1_being_the_worst_mood_::text
    END as ipc_s2_on_a_scale_of_1_to_10_with_1_being_the_worst_mood_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_was_this_in_the_context_of_an_argument_or_disagree')
        THEN 'REDACTED'
        ELSE ipc_s2_was_this_in_the_context_of_an_argument_or_disagree::text
    END as ipc_s2_was_this_in_the_context_of_an_argument_or_disagree,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_3d2ccf9a0e7b48a0854044d43b6dacfe')
        THEN 'REDACTED'
        ELSE ipc_s2_3d2ccf9a0e7b48a0854044d43b6dacfe::text
    END as ipc_s2_3d2ccf9a0e7b48a0854044d43b6dacfe,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_would_you_prefer_to_focus_on_this_new_problem_area_28')
        THEN 'REDACTED'
        ELSE ipc_s2_would_you_prefer_to_focus_on_this_new_problem_area_28::text
    END as ipc_s2_would_you_prefer_to_focus_on_this_new_problem_area_28,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_6fb0c7a5a21143ecaf7b5f1079399133')
        THEN 'REDACTED'
        ELSE ipc_s2_6fb0c7a5a21143ecaf7b5f1079399133::text
    END as ipc_s2_6fb0c7a5a21143ecaf7b5f1079399133,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_observation_id_of_mood_rating_session_1_score')
        THEN 'REDACTED'
        ELSE ipc_s2_observation_id_of_mood_rating_session_1_score::text
    END as ipc_s2_observation_id_of_mood_rating_session_1_score,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_did_you_ever_hit_kick_push_or_slap_the_person')
        THEN 'REDACTED'
        ELSE ipc_s2_did_you_ever_hit_kick_push_or_slap_the_person::text
    END as ipc_s2_did_you_ever_hit_kick_push_or_slap_the_person,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_do_you_still_think_that')
        THEN 'REDACTED'
        ELSE ipc_s2_do_you_still_think_that::text
    END as ipc_s2_do_you_still_think_that,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_mood_rating_total')
        THEN 'REDACTED'
        ELSE ipc_s2_mood_rating_total::text
    END as ipc_s2_mood_rating_total,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_232484cf1aee4bfaa66e61494587a16a')
        THEN 'REDACTED'
        ELSE ipc_s2_232484cf1aee4bfaa66e61494587a16a::text
    END as ipc_s2_232484cf1aee4bfaa66e61494587a16a,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_9ed77f2a0d5a434db153d8deadfbc3f8')
        THEN 'REDACTED'
        ELSE ipc_s2_9ed77f2a0d5a434db153d8deadfbc3f8::text
    END as ipc_s2_9ed77f2a0d5a434db153d8deadfbc3f8,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_3bfee97a6a0e4cd0bdb77ffa8d36191f')
        THEN 'REDACTED'
        ELSE ipc_s2_3bfee97a6a0e4cd0bdb77ffa8d36191f::text
    END as ipc_s2_3bfee97a6a0e4cd0bdb77ffa8d36191f,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_was_this_in_the_context_of_an_argument_or_disagree_37')
        THEN 'REDACTED'
        ELSE ipc_s2_was_this_in_the_context_of_an_argument_or_disagree_37::text
    END as ipc_s2_was_this_in_the_context_of_an_argument_or_disagree_37,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_example_guiding_prompts_not_all_questions_need_to_')
        THEN 'REDACTED'
        ELSE ipc_s2_example_guiding_prompts_not_all_questions_need_to_::text
    END as ipc_s2_example_guiding_prompts_not_all_questions_need_to_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_example_guiding_prompts_not_all_questions_need_to__39')
        THEN 'REDACTED'
        ELSE ipc_s2_example_guiding_prompts_not_all_questions_need_to__39::text
    END as ipc_s2_example_guiding_prompts_not_all_questions_need_to__39,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_example_guiding_prompts_not_all_questions_need_to__40')
        THEN 'REDACTED'
        ELSE ipc_s2_example_guiding_prompts_not_all_questions_need_to__40::text
    END as ipc_s2_example_guiding_prompts_not_all_questions_need_to__40,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_example_guiding_prompts_not_all_questions_need_to__41')
        THEN 'REDACTED'
        ELSE ipc_s2_example_guiding_prompts_not_all_questions_need_to__41::text
    END as ipc_s2_example_guiding_prompts_not_all_questions_need_to__41,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_287593c3417445afa99871d249230943')
        THEN 'REDACTED'
        ELSE ipc_s2_287593c3417445afa99871d249230943::text
    END as ipc_s2_287593c3417445afa99871d249230943,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_9cb58d5b0f2c48f095d2d861ab30c2a7')
        THEN 'REDACTED'
        ELSE ipc_s2_9cb58d5b0f2c48f095d2d861ab30c2a7::text
    END as ipc_s2_9cb58d5b0f2c48f095d2d861ab30c2a7,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_3f63637c1de24851a088c3b08152447e')
        THEN 'REDACTED'
        ELSE ipc_s2_3f63637c1de24851a088c3b08152447e::text
    END as ipc_s2_3f63637c1de24851a088c3b08152447e,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_50cf6a9ca8374531a13a47381657c2b3')
        THEN 'REDACTED'
        ELSE ipc_s2_50cf6a9ca8374531a13a47381657c2b3::text
    END as ipc_s2_50cf6a9ca8374531a13a47381657c2b3,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_665e1595f16948159d24dac265f1809b')
        THEN 'REDACTED'
        ELSE ipc_s2_665e1595f16948159d24dac265f1809b::text
    END as ipc_s2_665e1595f16948159d24dac265f1809b,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_bc8f199a59f04184864db1c0befd26df')
        THEN 'REDACTED'
        ELSE ipc_s2_bc8f199a59f04184864db1c0befd26df::text
    END as ipc_s2_bc8f199a59f04184864db1c0befd26df,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_e1755ad4b33a44a3b5624206a582c28a')
        THEN 'REDACTED'
        ELSE ipc_s2_e1755ad4b33a44a3b5624206a582c28a::text
    END as ipc_s2_e1755ad4b33a44a3b5624206a582c28a,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_e17756cd48614dde859cf90241977246')
        THEN 'REDACTED'
        ELSE ipc_s2_e17756cd48614dde859cf90241977246::text
    END as ipc_s2_e17756cd48614dde859cf90241977246,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_e6befba36d2547ae9cac409dfae832a2')
        THEN 'REDACTED'
        ELSE ipc_s2_e6befba36d2547ae9cac409dfae832a2::text
    END as ipc_s2_e6befba36d2547ae9cac409dfae832a2,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_is_any_of_the_2_question_above_is_true')
        THEN 'REDACTED'
        ELSE ipc_s2_is_any_of_the_2_question_above_is_true::text
    END as ipc_s2_is_any_of_the_2_question_above_is_true,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_total_score')
        THEN 'REDACTED'
        ELSE ipc_s2_total_score::text
    END as ipc_s2_total_score,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_2ef63cf4b37e4216a8e1db6d1b91cc2a')
        THEN 'REDACTED'
        ELSE ipc_s2_2ef63cf4b37e4216a8e1db6d1b91cc2a::text
    END as ipc_s2_2ef63cf4b37e4216a8e1db6d1b91cc2a,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_3e0b76f9090243bab8580b1505e9d168')
        THEN 'REDACTED'
        ELSE ipc_s2_3e0b76f9090243bab8580b1505e9d168::text
    END as ipc_s2_3e0b76f9090243bab8580b1505e9d168,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_9cc2834b0d2843d08811509995cfcdb8')
        THEN 'REDACTED'
        ELSE ipc_s2_9cc2834b0d2843d08811509995cfcdb8::text
    END as ipc_s2_9cc2834b0d2843d08811509995cfcdb8,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_a7f10195b8594670abbca2498c4fe45f')
        THEN 'REDACTED'
        ELSE ipc_s2_a7f10195b8594670abbca2498c4fe45f::text
    END as ipc_s2_a7f10195b8594670abbca2498c4fe45f,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_example_guiding_prompts_not_all_questions_need_to__57')
        THEN 'REDACTED'
        ELSE ipc_s2_example_guiding_prompts_not_all_questions_need_to__57::text
    END as ipc_s2_example_guiding_prompts_not_all_questions_need_to__57,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_f9543e3ccbff4d2ab3dfbf96e509d9ca')
        THEN 'REDACTED'
        ELSE ipc_s2_f9543e3ccbff4d2ab3dfbf96e509d9ca::text
    END as ipc_s2_f9543e3ccbff4d2ab3dfbf96e509d9ca,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_547a07c2f94c4956997f0439626d0251')
        THEN 'REDACTED'
        ELSE ipc_s2_547a07c2f94c4956997f0439626d0251::text
    END as ipc_s2_547a07c2f94c4956997f0439626d0251,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_6c1b2aa7f2444d5abfc6e0faef33bfe6')
        THEN 'REDACTED'
        ELSE ipc_s2_6c1b2aa7f2444d5abfc6e0faef33bfe6::text
    END as ipc_s2_6c1b2aa7f2444d5abfc6e0faef33bfe6,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_a77f07b379134708a35414e503a948f1')
        THEN 'REDACTED'
        ELSE ipc_s2_a77f07b379134708a35414e503a948f1::text
    END as ipc_s2_a77f07b379134708a35414e503a948f1,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_example_skills')
        THEN 'REDACTED'
        ELSE ipc_s2_example_skills::text
    END as ipc_s2_example_skills,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_example_skills_63')
        THEN 'REDACTED'
        ELSE ipc_s2_example_skills_63::text
    END as ipc_s2_example_skills_63,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_bcb8b5c0bcd84a078fe39d59916e8b7c')
        THEN 'REDACTED'
        ELSE ipc_s2_bcb8b5c0bcd84a078fe39d59916e8b7c::text
    END as ipc_s2_bcb8b5c0bcd84a078fe39d59916e8b7c,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_eb8828db167447e7ba9ae3c161160b65')
        THEN 'REDACTED'
        ELSE ipc_s2_eb8828db167447e7ba9ae3c161160b65::text
    END as ipc_s2_eb8828db167447e7ba9ae3c161160b65,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_example_skills_66')
        THEN 'REDACTED'
        ELSE ipc_s2_example_skills_66::text
    END as ipc_s2_example_skills_66,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_4c95559d86ff4b87a271e52388072a53')
        THEN 'REDACTED'
        ELSE ipc_s2_4c95559d86ff4b87a271e52388072a53::text
    END as ipc_s2_4c95559d86ff4b87a271e52388072a53,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_bed17079d1e54bbeaf073d7b6a86243e')
        THEN 'REDACTED'
        ELSE ipc_s2_bed17079d1e54bbeaf073d7b6a86243e::text
    END as ipc_s2_bed17079d1e54bbeaf073d7b6a86243e,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_example_guiding_prompts_not_all_questions_need_to__69')
        THEN 'REDACTED'
        ELSE ipc_s2_example_guiding_prompts_not_all_questions_need_to__69::text
    END as ipc_s2_example_guiding_prompts_not_all_questions_need_to__69,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_example_guiding_prompts_not_all_questions_need_to__70')
        THEN 'REDACTED'
        ELSE ipc_s2_example_guiding_prompts_not_all_questions_need_to__70::text
    END as ipc_s2_example_guiding_prompts_not_all_questions_need_to__70,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_example_guiding_prompts_not_all_questions_need_to__71')
        THEN 'REDACTED'
        ELSE ipc_s2_example_guiding_prompts_not_all_questions_need_to__71::text
    END as ipc_s2_example_guiding_prompts_not_all_questions_need_to__71,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_example_skills_72')
        THEN 'REDACTED'
        ELSE ipc_s2_example_skills_72::text
    END as ipc_s2_example_skills_72,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_c91209cc1b2c43f083f04704418ae2d5')
        THEN 'REDACTED'
        ELSE ipc_s2_c91209cc1b2c43f083f04704418ae2d5::text
    END as ipc_s2_c91209cc1b2c43f083f04704418ae2d5,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_example_guiding_prompts_not_all_questions_need_to__74')
        THEN 'REDACTED'
        ELSE ipc_s2_example_guiding_prompts_not_all_questions_need_to__74::text
    END as ipc_s2_example_guiding_prompts_not_all_questions_need_to__74,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_example_guiding_prompts_not_all_questions_need_to__75')
        THEN 'REDACTED'
        ELSE ipc_s2_example_guiding_prompts_not_all_questions_need_to__75::text
    END as ipc_s2_example_guiding_prompts_not_all_questions_need_to__75,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_example_guiding_prompts_not_all_questions_need_to__76')
        THEN 'REDACTED'
        ELSE ipc_s2_example_guiding_prompts_not_all_questions_need_to__76::text
    END as ipc_s2_example_guiding_prompts_not_all_questions_need_to__76,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_example_guiding_prompts_not_all_questions_need_to__77')
        THEN 'REDACTED'
        ELSE ipc_s2_example_guiding_prompts_not_all_questions_need_to__77::text
    END as ipc_s2_example_guiding_prompts_not_all_questions_need_to__77,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_example_guiding_prompts_not_all_questions_need_to__78')
        THEN 'REDACTED'
        ELSE ipc_s2_example_guiding_prompts_not_all_questions_need_to__78::text
    END as ipc_s2_example_guiding_prompts_not_all_questions_need_to__78,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_example_guiding_prompts_not_all_questions_need_to__79')
        THEN 'REDACTED'
        ELSE ipc_s2_example_guiding_prompts_not_all_questions_need_to__79::text
    END as ipc_s2_example_guiding_prompts_not_all_questions_need_to__79,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_example_guiding_prompts_not_all_questions_need_to__80')
        THEN 'REDACTED'
        ELSE ipc_s2_example_guiding_prompts_not_all_questions_need_to__80::text
    END as ipc_s2_example_guiding_prompts_not_all_questions_need_to__80,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_example_guiding_prompts_not_all_questions_need_to__81')
        THEN 'REDACTED'
        ELSE ipc_s2_example_guiding_prompts_not_all_questions_need_to__81::text
    END as ipc_s2_example_guiding_prompts_not_all_questions_need_to__81,
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
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'did_you_ever_hit_kick_slap_or_push_the_person')
        THEN 'REDACTED'
        ELSE did_you_ever_hit_kick_slap_or_push_the_person::text
    END as did_you_ever_hit_kick_slap_or_push_the_person,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-id-013')
        THEN 'REDACTED'
        ELSE "task-id-013"::text
    END as "task-id-013",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-id-014')
        THEN 'REDACTED'
        ELSE "task-id-014"::text
    END as "task-id-014",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-id-015')
        THEN 'REDACTED'
        ELSE "task-id-015"::text
    END as "task-id-015",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-id-016')
        THEN 'REDACTED'
        ELSE "task-id-016"::text
    END as "task-id-016",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-id-018')
        THEN 'REDACTED'
        ELSE "task-id-018"::text
    END as "task-id-018",
        CURRENT_TIMESTAMP as anonymized_at

from source_data
  );