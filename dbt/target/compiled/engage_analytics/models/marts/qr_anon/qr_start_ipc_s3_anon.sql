

-- Anonymized view for qr_start_ipc_s3 with PII fields masked based on questionnaire_metadata.anon flag
-- Auto-generated to mask fields marked as anon=TRUE in questionnaire_metadata

with metadata_pii as (
    select distinct
        question_alias
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where questionnaire_id in ('Questionnaire/208')
    and anon = 'TRUE'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_start_ipc_s3"
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
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_phq9_total')
        THEN 'REDACTED'
        ELSE ipc_s3_phq9_total::text
    END as ipc_s3_phq9_total,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_gad7_total')
        THEN 'REDACTED'
        ELSE ipc_s3_gad7_total::text
    END as ipc_s3_gad7_total,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_ptsd_total')
        THEN 'REDACTED'
        ELSE ipc_s3_ptsd_total::text
    END as ipc_s3_ptsd_total,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_ptsd_total_meaning')
        THEN 'REDACTED'
        ELSE ipc_s3_ptsd_total_meaning::text
    END as ipc_s3_ptsd_total_meaning,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_taskid_session_3_navigator')
        THEN 'REDACTED'
        ELSE ipc_s3_taskid_session_3_navigator::text
    END as ipc_s3_taskid_session_3_navigator,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_7ff4b5cb2c44434b90e985c61a858097')
        THEN 'REDACTED'
        ELSE ipc_s3_7ff4b5cb2c44434b90e985c61a858097::text
    END as ipc_s3_7ff4b5cb2c44434b90e985c61a858097,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_taskid_session_4_navigator')
        THEN 'REDACTED'
        ELSE ipc_s3_taskid_session_4_navigator::text
    END as ipc_s3_taskid_session_4_navigator,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_in_what_format_did_you_deliver_this_session_with_t')
        THEN 'REDACTED'
        ELSE ipc_s3_in_what_format_did_you_deliver_this_session_with_t::text
    END as ipc_s3_in_what_format_did_you_deliver_this_session_with_t,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_work_at_home_you_assigned_to_the_client_in_this_se')
        THEN 'REDACTED'
        ELSE ipc_s3_work_at_home_you_assigned_to_the_client_in_this_se::text
    END as ipc_s3_work_at_home_you_assigned_to_the_client_in_this_se,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_taskid_ipc_session_3_provider_pdf')
        THEN 'REDACTED'
        ELSE ipc_s3_taskid_ipc_session_3_provider_pdf::text
    END as ipc_s3_taskid_ipc_session_3_provider_pdf,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_did_you_complete_the_required_assessments_and_the_')
        THEN 'REDACTED'
        ELSE ipc_s3_did_you_complete_the_required_assessments_and_the_::text
    END as ipc_s3_did_you_complete_the_required_assessments_and_the_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_taskid_ipc_session_3_client_pdf')
        THEN 'REDACTED'
        ELSE ipc_s3_taskid_ipc_session_3_client_pdf::text
    END as ipc_s3_taskid_ipc_session_3_client_pdf,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_questions_asked_in_session_2')
        THEN 'REDACTED'
        ELSE ipc_s3_questions_asked_in_session_2::text
    END as ipc_s3_questions_asked_in_session_2,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_encounter_id_of_ipc_session_3')
        THEN 'REDACTED'
        ELSE ipc_s3_encounter_id_of_ipc_session_3::text
    END as ipc_s3_encounter_id_of_ipc_session_3,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_on_a_scale_of_1_to_10_with_1_being_the_worst_mood_')
        THEN 'REDACTED'
        ELSE ipc_s3_on_a_scale_of_1_to_10_with_1_being_the_worst_mood_::text
    END as ipc_s3_on_a_scale_of_1_to_10_with_1_being_the_worst_mood_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_questions_asked_in_session_2_16')
        THEN 'REDACTED'
        ELSE ipc_s3_questions_asked_in_session_2_16::text
    END as ipc_s3_questions_asked_in_session_2_16,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_would_you_prefer_to_focus_on_this_new_problem_area')
        THEN 'REDACTED'
        ELSE ipc_s3_would_you_prefer_to_focus_on_this_new_problem_area::text
    END as ipc_s3_would_you_prefer_to_focus_on_this_new_problem_area,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_questions_asked_in_session_2_18')
        THEN 'REDACTED'
        ELSE ipc_s3_questions_asked_in_session_2_18::text
    END as ipc_s3_questions_asked_in_session_2_18,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_observation_id_of_mood_rating_session_3_score')
        THEN 'REDACTED'
        ELSE ipc_s3_observation_id_of_mood_rating_session_3_score::text
    END as ipc_s3_observation_id_of_mood_rating_session_3_score,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_1b59ddeb483d4b1baf53f3942204589a')
        THEN 'REDACTED'
        ELSE ipc_s3_1b59ddeb483d4b1baf53f3942204589a::text
    END as ipc_s3_1b59ddeb483d4b1baf53f3942204589a,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_did_you_ever_hit_kick_push_or_slap_the_personno')
        THEN 'REDACTED'
        ELSE ipc_s3_did_you_ever_hit_kick_push_or_slap_the_personno::text
    END as ipc_s3_did_you_ever_hit_kick_push_or_slap_the_personno,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_mood_rating_total')
        THEN 'REDACTED'
        ELSE ipc_s3_mood_rating_total::text
    END as ipc_s3_mood_rating_total,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_questions_asked_in_session_2_23')
        THEN 'REDACTED'
        ELSE ipc_s3_questions_asked_in_session_2_23::text
    END as ipc_s3_questions_asked_in_session_2_23,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_4554f4b2d15e47e7a10dd77939dd3afc')
        THEN 'REDACTED'
        ELSE ipc_s3_4554f4b2d15e47e7a10dd77939dd3afc::text
    END as ipc_s3_4554f4b2d15e47e7a10dd77939dd3afc,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_questions_asked_in_session_2_25')
        THEN 'REDACTED'
        ELSE ipc_s3_questions_asked_in_session_2_25::text
    END as ipc_s3_questions_asked_in_session_2_25,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_f9a9f4928eb84c718ec6f80ab596430b')
        THEN 'REDACTED'
        ELSE ipc_s3_f9a9f4928eb84c718ec6f80ab596430b::text
    END as ipc_s3_f9a9f4928eb84c718ec6f80ab596430b,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_example_guiding_prompts_not_all_questions_need_to_')
        THEN 'REDACTED'
        ELSE ipc_s3_example_guiding_prompts_not_all_questions_need_to_::text
    END as ipc_s3_example_guiding_prompts_not_all_questions_need_to_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_example_guiding_prompts_not_all_questions_need_to__28')
        THEN 'REDACTED'
        ELSE ipc_s3_example_guiding_prompts_not_all_questions_need_to__28::text
    END as ipc_s3_example_guiding_prompts_not_all_questions_need_to__28,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_example_guiding_prompts_not_all_questions_need_to__29')
        THEN 'REDACTED'
        ELSE ipc_s3_example_guiding_prompts_not_all_questions_need_to__29::text
    END as ipc_s3_example_guiding_prompts_not_all_questions_need_to__29,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_example_guiding_prompts_not_all_questions_need_to__30')
        THEN 'REDACTED'
        ELSE ipc_s3_example_guiding_prompts_not_all_questions_need_to__30::text
    END as ipc_s3_example_guiding_prompts_not_all_questions_need_to__30,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_9cb58d5b0f2c48f095d2d861ab30c2a7')
        THEN 'REDACTED'
        ELSE ipc_s3_9cb58d5b0f2c48f095d2d861ab30c2a7::text
    END as ipc_s3_9cb58d5b0f2c48f095d2d861ab30c2a7,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_59744d4501004d62b1055cbecd973ffd')
        THEN 'REDACTED'
        ELSE ipc_s3_59744d4501004d62b1055cbecd973ffd::text
    END as ipc_s3_59744d4501004d62b1055cbecd973ffd,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_6b5fa4342fc34e4bad1189f6260a81f2')
        THEN 'REDACTED'
        ELSE ipc_s3_6b5fa4342fc34e4bad1189f6260a81f2::text
    END as ipc_s3_6b5fa4342fc34e4bad1189f6260a81f2,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_77fc70ba6cac494cb27a77ebce2289e5')
        THEN 'REDACTED'
        ELSE ipc_s3_77fc70ba6cac494cb27a77ebce2289e5::text
    END as ipc_s3_77fc70ba6cac494cb27a77ebce2289e5,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_3f63637c1de24851a088c3b08152447e')
        THEN 'REDACTED'
        ELSE ipc_s3_3f63637c1de24851a088c3b08152447e::text
    END as ipc_s3_3f63637c1de24851a088c3b08152447e,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_50cf6a9ca8374531a13a47381657c2b3')
        THEN 'REDACTED'
        ELSE ipc_s3_50cf6a9ca8374531a13a47381657c2b3::text
    END as ipc_s3_50cf6a9ca8374531a13a47381657c2b3,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_has_anything_changed_since_the_last_time_we_met')
        THEN 'REDACTED'
        ELSE ipc_s3_has_anything_changed_since_the_last_time_we_met::text
    END as ipc_s3_has_anything_changed_since_the_last_time_we_met,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_e6befba36d2547ae9cac409dfae832a2')
        THEN 'REDACTED'
        ELSE ipc_s3_e6befba36d2547ae9cac409dfae832a2::text
    END as ipc_s3_e6befba36d2547ae9cac409dfae832a2,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_total_score')
        THEN 'REDACTED'
        ELSE ipc_s3_total_score::text
    END as ipc_s3_total_score,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_questions_asked_in_session_2_40')
        THEN 'REDACTED'
        ELSE ipc_s3_questions_asked_in_session_2_40::text
    END as ipc_s3_questions_asked_in_session_2_40,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_4ad7384fa3324e4193180d3f9e540ee7')
        THEN 'REDACTED'
        ELSE ipc_s3_4ad7384fa3324e4193180d3f9e540ee7::text
    END as ipc_s3_4ad7384fa3324e4193180d3f9e540ee7,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_stage_identified_in_session_2')
        THEN 'REDACTED'
        ELSE ipc_s3_stage_identified_in_session_2::text
    END as ipc_s3_stage_identified_in_session_2,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_questions_asked_in_session_2_43')
        THEN 'REDACTED'
        ELSE ipc_s3_questions_asked_in_session_2_43::text
    END as ipc_s3_questions_asked_in_session_2_43,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_questions_asked_in_session_2_44')
        THEN 'REDACTED'
        ELSE ipc_s3_questions_asked_in_session_2_44::text
    END as ipc_s3_questions_asked_in_session_2_44,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_questions_asked_in_session_2_45')
        THEN 'REDACTED'
        ELSE ipc_s3_questions_asked_in_session_2_45::text
    END as ipc_s3_questions_asked_in_session_2_45,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_b24118e231c54e23af3a1722af72c299')
        THEN 'REDACTED'
        ELSE ipc_s3_b24118e231c54e23af3a1722af72c299::text
    END as ipc_s3_b24118e231c54e23af3a1722af72c299,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_68b3cbf5b9ef4da49ac1bcbc20f04473')
        THEN 'REDACTED'
        ELSE ipc_s3_68b3cbf5b9ef4da49ac1bcbc20f04473::text
    END as ipc_s3_68b3cbf5b9ef4da49ac1bcbc20f04473,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_9e995f48b09249b2a9e4c7e754e446e5')
        THEN 'REDACTED'
        ELSE ipc_s3_9e995f48b09249b2a9e4c7e754e446e5::text
    END as ipc_s3_9e995f48b09249b2a9e4c7e754e446e5,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_example_guiding_prompts_not_all_questions_need_to__49')
        THEN 'REDACTED'
        ELSE ipc_s3_example_guiding_prompts_not_all_questions_need_to__49::text
    END as ipc_s3_example_guiding_prompts_not_all_questions_need_to__49,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_e1755ad4b33a44a3b5624206a582c28a')
        THEN 'REDACTED'
        ELSE ipc_s3_e1755ad4b33a44a3b5624206a582c28a::text
    END as ipc_s3_e1755ad4b33a44a3b5624206a582c28a,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_e8b8e5504d6542beacb92f4ac17c7f49')
        THEN 'REDACTED'
        ELSE ipc_s3_e8b8e5504d6542beacb92f4ac17c7f49::text
    END as ipc_s3_e8b8e5504d6542beacb92f4ac17c7f49,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_example_skills')
        THEN 'REDACTED'
        ELSE ipc_s3_example_skills::text
    END as ipc_s3_example_skills,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_example_skills_53')
        THEN 'REDACTED'
        ELSE ipc_s3_example_skills_53::text
    END as ipc_s3_example_skills_53,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_bcb8b5c0bcd84a078fe39d59916e8b7c')
        THEN 'REDACTED'
        ELSE ipc_s3_bcb8b5c0bcd84a078fe39d59916e8b7c::text
    END as ipc_s3_bcb8b5c0bcd84a078fe39d59916e8b7c,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_example_skills_55')
        THEN 'REDACTED'
        ELSE ipc_s3_example_skills_55::text
    END as ipc_s3_example_skills_55,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_questions_asked_in_session_2_56')
        THEN 'REDACTED'
        ELSE ipc_s3_questions_asked_in_session_2_56::text
    END as ipc_s3_questions_asked_in_session_2_56,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_questions_asked_in_session_2_57')
        THEN 'REDACTED'
        ELSE ipc_s3_questions_asked_in_session_2_57::text
    END as ipc_s3_questions_asked_in_session_2_57,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_questions_asked_in_session_2_58')
        THEN 'REDACTED'
        ELSE ipc_s3_questions_asked_in_session_2_58::text
    END as ipc_s3_questions_asked_in_session_2_58,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_f9543e3ccbff4d2ab3dfbf96e509d9ca')
        THEN 'REDACTED'
        ELSE ipc_s3_f9543e3ccbff4d2ab3dfbf96e509d9ca::text
    END as ipc_s3_f9543e3ccbff4d2ab3dfbf96e509d9ca,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_questions_asked_in_session_2_60')
        THEN 'REDACTED'
        ELSE ipc_s3_questions_asked_in_session_2_60::text
    END as ipc_s3_questions_asked_in_session_2_60,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_920de190557a423391a34bf293f18214')
        THEN 'REDACTED'
        ELSE ipc_s3_920de190557a423391a34bf293f18214::text
    END as ipc_s3_920de190557a423391a34bf293f18214,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_95121ef1bfe540ce841c524334644148')
        THEN 'REDACTED'
        ELSE ipc_s3_95121ef1bfe540ce841c524334644148::text
    END as ipc_s3_95121ef1bfe540ce841c524334644148,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_e1a13d53b867440f8622bccebf54f176')
        THEN 'REDACTED'
        ELSE ipc_s3_e1a13d53b867440f8622bccebf54f176::text
    END as ipc_s3_e1a13d53b867440f8622bccebf54f176,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_547a07c2f94c4956997f0439626d0251')
        THEN 'REDACTED'
        ELSE ipc_s3_547a07c2f94c4956997f0439626d0251::text
    END as ipc_s3_547a07c2f94c4956997f0439626d0251,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_8ac792f53f3c49d3a80063c662bf00cd')
        THEN 'REDACTED'
        ELSE ipc_s3_8ac792f53f3c49d3a80063c662bf00cd::text
    END as ipc_s3_8ac792f53f3c49d3a80063c662bf00cd,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_example_guiding_prompts_not_all_questions_need_to__66')
        THEN 'REDACTED'
        ELSE ipc_s3_example_guiding_prompts_not_all_questions_need_to__66::text
    END as ipc_s3_example_guiding_prompts_not_all_questions_need_to__66,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_example_guiding_prompts_not_all_questions_need_to__67')
        THEN 'REDACTED'
        ELSE ipc_s3_example_guiding_prompts_not_all_questions_need_to__67::text
    END as ipc_s3_example_guiding_prompts_not_all_questions_need_to__67,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_example_guiding_prompts_not_all_questions_need_to__68')
        THEN 'REDACTED'
        ELSE ipc_s3_example_guiding_prompts_not_all_questions_need_to__68::text
    END as ipc_s3_example_guiding_prompts_not_all_questions_need_to__68,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_a77f07b379134708a35414e503a948f1')
        THEN 'REDACTED'
        ELSE ipc_s3_a77f07b379134708a35414e503a948f1::text
    END as ipc_s3_a77f07b379134708a35414e503a948f1,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_example_skills_70')
        THEN 'REDACTED'
        ELSE ipc_s3_example_skills_70::text
    END as ipc_s3_example_skills_70,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_questions_asked_in_session_2_71')
        THEN 'REDACTED'
        ELSE ipc_s3_questions_asked_in_session_2_71::text
    END as ipc_s3_questions_asked_in_session_2_71,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_4c95559d86ff4b87a271e52388072a53')
        THEN 'REDACTED'
        ELSE ipc_s3_4c95559d86ff4b87a271e52388072a53::text
    END as ipc_s3_4c95559d86ff4b87a271e52388072a53,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_example_guiding_prompts_not_all_questions_need_to__73')
        THEN 'REDACTED'
        ELSE ipc_s3_example_guiding_prompts_not_all_questions_need_to__73::text
    END as ipc_s3_example_guiding_prompts_not_all_questions_need_to__73,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_example_guiding_prompts_not_all_questions_need_to__74')
        THEN 'REDACTED'
        ELSE ipc_s3_example_guiding_prompts_not_all_questions_need_to__74::text
    END as ipc_s3_example_guiding_prompts_not_all_questions_need_to__74,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_example_guiding_prompts_not_all_questions_need_to__75')
        THEN 'REDACTED'
        ELSE ipc_s3_example_guiding_prompts_not_all_questions_need_to__75::text
    END as ipc_s3_example_guiding_prompts_not_all_questions_need_to__75,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_example_guiding_prompts_not_all_questions_need_to__76')
        THEN 'REDACTED'
        ELSE ipc_s3_example_guiding_prompts_not_all_questions_need_to__76::text
    END as ipc_s3_example_guiding_prompts_not_all_questions_need_to__76,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_questions_asked_in_session_2_77')
        THEN 'REDACTED'
        ELSE ipc_s3_questions_asked_in_session_2_77::text
    END as ipc_s3_questions_asked_in_session_2_77,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_8c82f7573f404b09bdf478ee376f75a8')
        THEN 'REDACTED'
        ELSE ipc_s3_8c82f7573f404b09bdf478ee376f75a8::text
    END as ipc_s3_8c82f7573f404b09bdf478ee376f75a8,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_c91209cc1b2c43f083f04704418ae2d5')
        THEN 'REDACTED'
        ELSE ipc_s3_c91209cc1b2c43f083f04704418ae2d5::text
    END as ipc_s3_c91209cc1b2c43f083f04704418ae2d5,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_example_guiding_prompts_not_all_questions_need_to__80')
        THEN 'REDACTED'
        ELSE ipc_s3_example_guiding_prompts_not_all_questions_need_to__80::text
    END as ipc_s3_example_guiding_prompts_not_all_questions_need_to__80,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_example_guiding_prompts_not_all_questions_need_to__81')
        THEN 'REDACTED'
        ELSE ipc_s3_example_guiding_prompts_not_all_questions_need_to__81::text
    END as ipc_s3_example_guiding_prompts_not_all_questions_need_to__81,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_example_guiding_prompts_not_all_questions_need_to__82')
        THEN 'REDACTED'
        ELSE ipc_s3_example_guiding_prompts_not_all_questions_need_to__82::text
    END as ipc_s3_example_guiding_prompts_not_all_questions_need_to__82,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_example_guiding_prompts_not_all_questions_need_to__83')
        THEN 'REDACTED'
        ELSE ipc_s3_example_guiding_prompts_not_all_questions_need_to__83::text
    END as ipc_s3_example_guiding_prompts_not_all_questions_need_to__83,
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
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'phq9_total_meaning')
        THEN 'REDACTED'
        ELSE phq9_total_meaning::text
    END as phq9_total_meaning,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'gad7_total_meaning')
        THEN 'REDACTED'
        ELSE gad7_total_meaning::text
    END as gad7_total_meaning,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'did_you_ever_hit_kick_slap_or_push_the_person')
        THEN 'REDACTED'
        ELSE did_you_ever_hit_kick_slap_or_push_the_person::text
    END as did_you_ever_hit_kick_slap_or_push_the_person,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'did_you_ever_hit_kick_slap_or_push_the_person_91')
        THEN 'REDACTED'
        ELSE did_you_ever_hit_kick_slap_or_push_the_person_91::text
    END as did_you_ever_hit_kick_slap_or_push_the_person_91,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-id-019')
        THEN 'REDACTED'
        ELSE "task-id-019"::text
    END as "task-id-019",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-id-020')
        THEN 'REDACTED'
        ELSE "task-id-020"::text
    END as "task-id-020",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-id-021')
        THEN 'REDACTED'
        ELSE "task-id-021"::text
    END as "task-id-021",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-id-022')
        THEN 'REDACTED'
        ELSE "task-id-022"::text
    END as "task-id-022",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-id-023')
        THEN 'REDACTED'
        ELSE "task-id-023"::text
    END as "task-id-023",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-id-024')
        THEN 'REDACTED'
        ELSE "task-id-024"::text
    END as "task-id-024",
        CURRENT_TIMESTAMP as anonymized_at

from source_data