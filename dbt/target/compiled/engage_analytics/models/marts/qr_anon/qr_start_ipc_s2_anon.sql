

-- Anonymized view for qr_start_ipc_s2
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_start_ipc_s2'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_start_ipc_s2"
)

select
    

    -- Hash patient and QR IDs for privacy
    MD5(qr_id::text) as qr_id_hash,
    questionnaire_id,
    MD5(COALESCE(subject_patient_id, '')::text) as subject_patient_id_hash,
    encounter_id,
    author_practitioner_id,
    practitioner_location_id,
    practitioner_organization_id,
    practitioner_id,
    practitioner_careteam_id,
    application_version,

    -- Process all other columns dynamically
    
        
    
        
    
        
    
        
    
        
    
        
    
        
    
        
    
        
    
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'communication_analysis_example_guiding_prompts_not_all_question' OR linkid = 'communication_analysis_example_guiding_prompts_not_all_question' OR short_name = 'communication_analysis_example_guiding_prompts_not_all_question')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "communication_analysis_example_guiding_prompts_not_all_question"
            END as "communication_analysis_example_guiding_prompts_not_all_question",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'role_play_example_guiding_prompts_not_all_questions_need_to_be_' OR linkid = 'role_play_example_guiding_prompts_not_all_questions_need_to_be_' OR short_name = 'role_play_example_guiding_prompts_not_all_questions_need_to_be_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "role_play_example_guiding_prompts_not_all_questions_need_to_be_"
            END as "role_play_example_guiding_prompts_not_all_questions_need_to_be_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_2_phq_9_total_meaning' OR linkid = 'ipc_session_2_phq_9_total_meaning' OR short_name = 'ipc_session_2_phq_9_total_meaning')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_2_phq_9_total_meaning"
            END as "ipc_session_2_phq_9_total_meaning",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'decision_analysis_example_guiding_prompts_not_all_questions_nee' OR linkid = 'decision_analysis_example_guiding_prompts_not_all_questions_nee' OR short_name = 'decision_analysis_example_guiding_prompts_not_all_questions_nee')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "decision_analysis_example_guiding_prompts_not_all_questions_nee"
            END as "decision_analysis_example_guiding_prompts_not_all_questions_nee",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'interpersonal_skills_building_example_skills' OR linkid = 'interpersonal_skills_building_example_skills' OR short_name = 'interpersonal_skills_building_example_skills')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "interpersonal_skills_building_example_skills"
            END as "interpersonal_skills_building_example_skills",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'work_at_home_example_guiding_prompts_not_all_questions_need_to_' OR linkid = 'work_at_home_example_guiding_prompts_not_all_questions_need_to_' OR short_name = 'work_at_home_example_guiding_prompts_not_all_questions_need_to_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "work_at_home_example_guiding_prompts_not_all_questions_need_to_"
            END as "work_at_home_example_guiding_prompts_not_all_questions_need_to_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_2_gad_7_total_meaning' OR linkid = 'ipc_session_2_gad_7_total_meaning' OR short_name = 'ipc_session_2_gad_7_total_meaning')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_2_gad_7_total_meaning"
            END as "ipc_session_2_gad_7_total_meaning",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'role_play_example_guiding_prompts_not_all_questions_need_to_b_2' OR linkid = 'role_play_example_guiding_prompts_not_all_questions_need_to_b_2' OR short_name = 'role_play_example_guiding_prompts_not_all_questions_need_to_b_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "role_play_example_guiding_prompts_not_all_questions_need_to_b_2"
            END as "role_play_example_guiding_prompts_not_all_questions_need_to_b_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'explore_the_problem_questions_asked_in_session_2' OR linkid = 'explore_the_problem_questions_asked_in_session_2' OR short_name = 'explore_the_problem_questions_asked_in_session_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "explore_the_problem_questions_asked_in_session_2"
            END as "explore_the_problem_questions_asked_in_session_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mood_rating_ipc_session_2' OR linkid = 'mood_rating_ipc_session_2' OR short_name = 'mood_rating_ipc_session_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mood_rating_ipc_session_2"
            END as "mood_rating_ipc_session_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'role_play_example_guiding_prompts_not_all_questions_need_to_b_3' OR linkid = 'role_play_example_guiding_prompts_not_all_questions_need_to_b_3' OR short_name = 'role_play_example_guiding_prompts_not_all_questions_need_to_b_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "role_play_example_guiding_prompts_not_all_questions_need_to_b_3"
            END as "role_play_example_guiding_prompts_not_all_questions_need_to_b_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'identified_stage_of_disagreement' OR linkid = 'identified_stage_of_disagreement' OR short_name = 'identified_stage_of_disagreement')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "identified_stage_of_disagreement"
            END as "identified_stage_of_disagreement",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'assess_for_violence_was_this_in_the_context_of_an_argument_or_d' OR linkid = 'assess_for_violence_was_this_in_the_context_of_an_argument_or_d' OR short_name = 'assess_for_violence_was_this_in_the_context_of_an_argument_or_d')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "assess_for_violence_was_this_in_the_context_of_an_argument_or_d"
            END as "assess_for_violence_was_this_in_the_context_of_an_argument_or_d",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'work_at_home_example_guiding_prompts_not_all_questions_need_t_2' OR linkid = 'work_at_home_example_guiding_prompts_not_all_questions_need_t_2' OR short_name = 'work_at_home_example_guiding_prompts_not_all_questions_need_t_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "work_at_home_example_guiding_prompts_not_all_questions_need_t_2"
            END as "work_at_home_example_guiding_prompts_not_all_questions_need_t_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_2_have_you_proposed_the_interpersonal_formulation_i' OR linkid = 'ipc_session_2_have_you_proposed_the_interpersonal_formulation_i' OR short_name = 'ipc_session_2_have_you_proposed_the_interpersonal_formulation_i')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_2_have_you_proposed_the_interpersonal_formulation_i"
            END as "ipc_session_2_have_you_proposed_the_interpersonal_formulation_i",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_92' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_92' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_92')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_92"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_92",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'impasse_questions_asked_in_session_2' OR linkid = 'impasse_questions_asked_in_session_2' OR short_name = 'impasse_questions_asked_in_session_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "impasse_questions_asked_in_session_2"
            END as "impasse_questions_asked_in_session_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'feelings_about_change_questions_asked_in_session_2' OR linkid = 'feelings_about_change_questions_asked_in_session_2' OR short_name = 'feelings_about_change_questions_asked_in_session_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "feelings_about_change_questions_asked_in_session_2"
            END as "feelings_about_change_questions_asked_in_session_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'explore_new_relationships_and_re_establish_old_relationships_an' OR linkid = 'explore_new_relationships_and_re_establish_old_relationships_an' OR short_name = 'explore_new_relationships_and_re_establish_old_relationships_an')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "explore_new_relationships_and_re_establish_old_relationships_an"
            END as "explore_new_relationships_and_re_establish_old_relationships_an",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'decision_analysis' OR linkid = 'decision_analysis' OR short_name = 'decision_analysis')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "decision_analysis"
            END as "decision_analysis",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_2' OR linkid = 'ipc_session_2' OR short_name = 'ipc_session_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_2"
            END as "ipc_session_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'decision_analysis_2' OR linkid = 'decision_analysis_2' OR short_name = 'decision_analysis_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "decision_analysis_2"
            END as "decision_analysis_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'communication_analysis_example_guiding_prompts_not_all_questi_2' OR linkid = 'communication_analysis_example_guiding_prompts_not_all_questi_2' OR short_name = 'communication_analysis_example_guiding_prompts_not_all_questi_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "communication_analysis_example_guiding_prompts_not_all_questi_2"
            END as "communication_analysis_example_guiding_prompts_not_all_questi_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_2_2' OR linkid = 'ipc_session_2_2' OR short_name = 'ipc_session_2_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_2_2"
            END as "ipc_session_2_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'work_at_home_example_guiding_prompts_not_all_questions_need_t_3' OR linkid = 'work_at_home_example_guiding_prompts_not_all_questions_need_t_3' OR short_name = 'work_at_home_example_guiding_prompts_not_all_questions_need_t_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "work_at_home_example_guiding_prompts_not_all_questions_need_t_3"
            END as "work_at_home_example_guiding_prompts_not_all_questions_need_t_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_2_in_what_format_did_you_deliver_this_session_with_' OR linkid = 'ipc_session_2_in_what_format_did_you_deliver_this_session_with_' OR short_name = 'ipc_session_2_in_what_format_did_you_deliver_this_session_with_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_2_in_what_format_did_you_deliver_this_session_with_"
            END as "ipc_session_2_in_what_format_did_you_deliver_this_session_with_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_93' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_93' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_93')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_93"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_93",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'assess_for_violence_was_this_in_the_context_of_an_argument_or_2' OR linkid = 'assess_for_violence_was_this_in_the_context_of_an_argument_or_2' OR short_name = 'assess_for_violence_was_this_in_the_context_of_an_argument_or_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "assess_for_violence_was_this_in_the_context_of_an_argument_or_2"
            END as "assess_for_violence_was_this_in_the_context_of_an_argument_or_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_2_do_you_agree_with_this' OR linkid = 'ipc_session_2_do_you_agree_with_this' OR short_name = 'ipc_session_2_do_you_agree_with_this')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_2_do_you_agree_with_this"
            END as "ipc_session_2_do_you_agree_with_this",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'role_play_example_guiding_prompts_not_all_questions_need_to_b_4' OR linkid = 'role_play_example_guiding_prompts_not_all_questions_need_to_b_4' OR short_name = 'role_play_example_guiding_prompts_not_all_questions_need_to_b_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "role_play_example_guiding_prompts_not_all_questions_need_to_b_4"
            END as "role_play_example_guiding_prompts_not_all_questions_need_to_b_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'the_old_role_questions_asked_in_session_2' OR linkid = 'the_old_role_questions_asked_in_session_2' OR short_name = 'the_old_role_questions_asked_in_session_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "the_old_role_questions_asked_in_session_2"
            END as "the_old_role_questions_asked_in_session_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_2_would_you_prefer_to_focus_on_this_new_problem_are' OR linkid = 'ipc_session_2_would_you_prefer_to_focus_on_this_new_problem_are' OR short_name = 'ipc_session_2_would_you_prefer_to_focus_on_this_new_problem_are')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_2_would_you_prefer_to_focus_on_this_new_problem_are"
            END as "ipc_session_2_would_you_prefer_to_focus_on_this_new_problem_are",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_94' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_94' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_94')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_94"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_94",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'the_new_role_questions_asked_in_session_2' OR linkid = 'the_new_role_questions_asked_in_session_2' OR short_name = 'the_new_role_questions_asked_in_session_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "the_new_role_questions_asked_in_session_2"
            END as "the_new_role_questions_asked_in_session_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'discuss_the_relationship_questions_asked_in_session_2' OR linkid = 'discuss_the_relationship_questions_asked_in_session_2' OR short_name = 'discuss_the_relationship_questions_asked_in_session_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "discuss_the_relationship_questions_asked_in_session_2"
            END as "discuss_the_relationship_questions_asked_in_session_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'assess_for_violence_is_any_of_the_2_question_above_is_true' OR linkid = 'assess_for_violence_is_any_of_the_2_question_above_is_true' OR short_name = 'assess_for_violence_is_any_of_the_2_question_above_is_true')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "assess_for_violence_is_any_of_the_2_question_above_is_true"
            END as "assess_for_violence_is_any_of_the_2_question_above_is_true",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'renegotiation_questions_asked_in_session_2' OR linkid = 'renegotiation_questions_asked_in_session_2' OR short_name = 'renegotiation_questions_asked_in_session_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "renegotiation_questions_asked_in_session_2"
            END as "renegotiation_questions_asked_in_session_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_2_3' OR linkid = 'ipc_session_2_3' OR short_name = 'ipc_session_2_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_2_3"
            END as "ipc_session_2_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'assess_for_violence_did_you_ever_hit_kick_push_or_slap_the_pers' OR linkid = 'assess_for_violence_did_you_ever_hit_kick_push_or_slap_the_pers' OR short_name = 'assess_for_violence_did_you_ever_hit_kick_push_or_slap_the_pers')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "assess_for_violence_did_you_ever_hit_kick_push_or_slap_the_pers"
            END as "assess_for_violence_did_you_ever_hit_kick_push_or_slap_the_pers",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mood_rating_ipc_session_2_total_score_2' OR linkid = 'mood_rating_ipc_session_2_total_score_2' OR short_name = 'mood_rating_ipc_session_2_total_score_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mood_rating_ipc_session_2_total_score_2"
            END as "mood_rating_ipc_session_2_total_score_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'survey_response_10' OR linkid = 'survey_response_10' OR short_name = 'survey_response_10')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "survey_response_10"
            END as "survey_response_10",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_2_phq_9_total' OR linkid = 'ipc_session_2_phq_9_total' OR short_name = 'ipc_session_2_phq_9_total')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_2_phq_9_total"
            END as "ipc_session_2_phq_9_total",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_2_summary_when_1' OR linkid = 'ipc_session_2_summary_when_1' OR short_name = 'ipc_session_2_summary_when_1')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_2_summary_when_1"
            END as "ipc_session_2_summary_when_1",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_2_most_distressing_symptoms' OR linkid = 'ipc_session_2_most_distressing_symptoms' OR short_name = 'ipc_session_2_most_distressing_symptoms')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_2_most_distressing_symptoms"
            END as "ipc_session_2_most_distressing_symptoms",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_2_selected_problem_area' OR linkid = 'ipc_session_2_selected_problem_area' OR short_name = 'ipc_session_2_selected_problem_area')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_2_selected_problem_area"
            END as "ipc_session_2_selected_problem_area",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_2_ptsd_total_meaning' OR linkid = 'ipc_session_2_ptsd_total_meaning' OR short_name = 'ipc_session_2_ptsd_total_meaning')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_2_ptsd_total_meaning"
            END as "ipc_session_2_ptsd_total_meaning",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_2_gad_7_total' OR linkid = 'ipc_session_2_gad_7_total' OR short_name = 'ipc_session_2_gad_7_total')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_2_gad_7_total"
            END as "ipc_session_2_gad_7_total",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'discuss_work_at_home_work_at_home_you_assigned_to_the_client_in' OR linkid = 'discuss_work_at_home_work_at_home_you_assigned_to_the_client_in' OR short_name = 'discuss_work_at_home_work_at_home_you_assigned_to_the_client_in')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "discuss_work_at_home_work_at_home_you_assigned_to_the_client_in"
            END as "discuss_work_at_home_work_at_home_you_assigned_to_the_client_in",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'go_to_the_interventions_tab_and_select_the_suicide_prevention_m' OR linkid = 'go_to_the_interventions_tab_and_select_the_suicide_prevention_m' OR short_name = 'go_to_the_interventions_tab_and_select_the_suicide_prevention_m')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "go_to_the_interventions_tab_and_select_the_suicide_prevention_m"
            END as "go_to_the_interventions_tab_and_select_the_suicide_prevention_m",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'explore_difficulties_in_relationships_questions_asked_in_sessio' OR linkid = 'explore_difficulties_in_relationships_questions_asked_in_sessio' OR short_name = 'explore_difficulties_in_relationships_questions_asked_in_sessio')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "explore_difficulties_in_relationships_questions_asked_in_sessio"
            END as "explore_difficulties_in_relationships_questions_asked_in_sessio",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'decision_analysis_example_guiding_prompts_not_all_questions_n_2' OR linkid = 'decision_analysis_example_guiding_prompts_not_all_questions_n_2' OR short_name = 'decision_analysis_example_guiding_prompts_not_all_questions_n_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "decision_analysis_example_guiding_prompts_not_all_questions_n_2"
            END as "decision_analysis_example_guiding_prompts_not_all_questions_n_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'disagreements_questions_asked_in_session_2' OR linkid = 'disagreements_questions_asked_in_session_2' OR short_name = 'disagreements_questions_asked_in_session_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "disagreements_questions_asked_in_session_2"
            END as "disagreements_questions_asked_in_session_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_95' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_95' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_95')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_95"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_95",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_96' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_96' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_96')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_96"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_96",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_2_4' OR linkid = 'ipc_session_2_4' OR short_name = 'ipc_session_2_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_2_4"
            END as "ipc_session_2_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'guiding_prompts_for_stages_of_disagreements' OR linkid = 'guiding_prompts_for_stages_of_disagreements' OR short_name = 'guiding_prompts_for_stages_of_disagreements')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "guiding_prompts_for_stages_of_disagreements"
            END as "guiding_prompts_for_stages_of_disagreements",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'dissolution_questions_asked_in_session_2' OR linkid = 'dissolution_questions_asked_in_session_2' OR short_name = 'dissolution_questions_asked_in_session_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "dissolution_questions_asked_in_session_2"
            END as "dissolution_questions_asked_in_session_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_2_do_you_still_think_that' OR linkid = 'ipc_session_2_do_you_still_think_that' OR short_name = 'ipc_session_2_do_you_still_think_that')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_2_do_you_still_think_that"
            END as "ipc_session_2_do_you_still_think_that",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_2_would_you_prefer_to_focus_on_this_new_problem_a_2' OR linkid = 'ipc_session_2_would_you_prefer_to_focus_on_this_new_problem_a_2' OR short_name = 'ipc_session_2_would_you_prefer_to_focus_on_this_new_problem_a_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_2_would_you_prefer_to_focus_on_this_new_problem_a_2"
            END as "ipc_session_2_would_you_prefer_to_focus_on_this_new_problem_a_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'interpersonal_skills_building_example_skills_2' OR linkid = 'interpersonal_skills_building_example_skills_2' OR short_name = 'interpersonal_skills_building_example_skills_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "interpersonal_skills_building_example_skills_2"
            END as "interpersonal_skills_building_example_skills_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_2_did_you_complete_the_required_assessment_s_and__2' OR linkid = 'ipc_session_2_did_you_complete_the_required_assessment_s_and__2' OR short_name = 'ipc_session_2_did_you_complete_the_required_assessment_s_and__2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_2_did_you_complete_the_required_assessment_s_and__2"
            END as "ipc_session_2_did_you_complete_the_required_assessment_s_and__2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_age_11' OR linkid = 'add_family_member_registration_calculated_age_11' OR short_name = 'add_family_member_registration_calculated_age_11')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_age_11"
            END as "add_family_member_registration_calculated_age_11",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'communication_analysis_example_guiding_prompts_not_all_questi_3' OR linkid = 'communication_analysis_example_guiding_prompts_not_all_questi_3' OR short_name = 'communication_analysis_example_guiding_prompts_not_all_questi_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "communication_analysis_example_guiding_prompts_not_all_questi_3"
            END as "communication_analysis_example_guiding_prompts_not_all_questi_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'discuss_the_death_questions_asked_in_session_2' OR linkid = 'discuss_the_death_questions_asked_in_session_2' OR short_name = 'discuss_the_death_questions_asked_in_session_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "discuss_the_death_questions_asked_in_session_2"
            END as "discuss_the_death_questions_asked_in_session_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'decision_analysis_3' OR linkid = 'decision_analysis_3' OR short_name = 'decision_analysis_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "decision_analysis_3"
            END as "decision_analysis_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'increase_activities_with_others_questions_asked_in_session_2' OR linkid = 'increase_activities_with_others_questions_asked_in_session_2' OR short_name = 'increase_activities_with_others_questions_asked_in_session_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "increase_activities_with_others_questions_asked_in_session_2"
            END as "increase_activities_with_others_questions_asked_in_session_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_2_5' OR linkid = 'ipc_session_2_5' OR short_name = 'ipc_session_2_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_2_5"
            END as "ipc_session_2_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_month_11' OR linkid = 'add_family_member_registration_calculated_month_11' OR short_name = 'add_family_member_registration_calculated_month_11')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_month_11"
            END as "add_family_member_registration_calculated_month_11",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_year_11' OR linkid = 'add_family_member_registration_calculated_year_11' OR short_name = 'add_family_member_registration_calculated_year_11')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_year_11"
            END as "add_family_member_registration_calculated_year_11",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_date_of_birth_11' OR linkid = 'add_family_member_registration_date_of_birth_11' OR short_name = 'add_family_member_registration_date_of_birth_11')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_date_of_birth_11"
            END as "add_family_member_registration_date_of_birth_11",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'assess_for_violence_did_the_person_ever_hit_kick_push_or_slap_y' OR linkid = 'assess_for_violence_did_the_person_ever_hit_kick_push_or_slap_y' OR short_name = 'assess_for_violence_did_the_person_ever_hit_kick_push_or_slap_y')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "assess_for_violence_did_the_person_ever_hit_kick_push_or_slap_y"
            END as "assess_for_violence_did_the_person_ever_hit_kick_push_or_slap_y",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'identified_stage_of_disagreement_has_there_been_a_change_in_the' OR linkid = 'identified_stage_of_disagreement_has_there_been_a_change_in_the' OR short_name = 'identified_stage_of_disagreement_has_there_been_a_change_in_the')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "identified_stage_of_disagreement_has_there_been_a_change_in_the"
            END as "identified_stage_of_disagreement_has_there_been_a_change_in_the",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'discuss_close_relationships_in_the_past_questions_asked_in_sess' OR linkid = 'discuss_close_relationships_in_the_past_questions_asked_in_sess' OR short_name = 'discuss_close_relationships_in_the_past_questions_asked_in_sess')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "discuss_close_relationships_in_the_past_questions_asked_in_sess"
            END as "discuss_close_relationships_in_the_past_questions_asked_in_sess",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'interpersonal_skills_building_example_skills_3' OR linkid = 'interpersonal_skills_building_example_skills_3' OR short_name = 'interpersonal_skills_building_example_skills_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "interpersonal_skills_building_example_skills_3"
            END as "interpersonal_skills_building_example_skills_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_97' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_97' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_97')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_97"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_97",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'decision_analysis_4' OR linkid = 'decision_analysis_4' OR short_name = 'decision_analysis_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "decision_analysis_4"
            END as "decision_analysis_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'communication_analysis_example_guiding_prompts_not_all_questi_4' OR linkid = 'communication_analysis_example_guiding_prompts_not_all_questi_4' OR short_name = 'communication_analysis_example_guiding_prompts_not_all_questi_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "communication_analysis_example_guiding_prompts_not_all_questi_4"
            END as "communication_analysis_example_guiding_prompts_not_all_questi_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'identify_social_settings_questions_asked_in_session_2' OR linkid = 'identify_social_settings_questions_asked_in_session_2' OR short_name = 'identify_social_settings_questions_asked_in_session_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "identify_social_settings_questions_asked_in_session_2"
            END as "identify_social_settings_questions_asked_in_session_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'decision_analysis_example_guiding_prompts_not_all_questions_n_3' OR linkid = 'decision_analysis_example_guiding_prompts_not_all_questions_n_3' OR short_name = 'decision_analysis_example_guiding_prompts_not_all_questions_n_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "decision_analysis_example_guiding_prompts_not_all_questions_n_3"
            END as "decision_analysis_example_guiding_prompts_not_all_questions_n_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'interpersonal_skills_building_example_skills_4' OR linkid = 'interpersonal_skills_building_example_skills_4' OR short_name = 'interpersonal_skills_building_example_skills_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "interpersonal_skills_building_example_skills_4"
            END as "interpersonal_skills_building_example_skills_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_98' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_98' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_98')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_98"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_98",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being__6' OR linkid = 'mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being__6' OR short_name = 'mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being__6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being__6"
            END as "mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being__6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'work_at_home_example_guiding_prompts_not_all_questions_need_t_4' OR linkid = 'work_at_home_example_guiding_prompts_not_all_questions_need_t_4' OR short_name = 'work_at_home_example_guiding_prompts_not_all_questions_need_t_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "work_at_home_example_guiding_prompts_not_all_questions_need_t_4"
            END as "work_at_home_example_guiding_prompts_not_all_questions_need_t_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'decision_analysis_example_guiding_prompts_not_all_questions_n_4' OR linkid = 'decision_analysis_example_guiding_prompts_not_all_questions_n_4' OR short_name = 'decision_analysis_example_guiding_prompts_not_all_questions_n_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "decision_analysis_example_guiding_prompts_not_all_questions_n_4"
            END as "decision_analysis_example_guiding_prompts_not_all_questions_n_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'guiding_prompts_for_disagreements' OR linkid = 'guiding_prompts_for_disagreements' OR short_name = 'guiding_prompts_for_disagreements')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "guiding_prompts_for_disagreements"
            END as "guiding_prompts_for_disagreements",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_99' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_99' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_99')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_99"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_99",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_100' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_100' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_100')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhir_100"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhir_100",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_101' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_101' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_101')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhir_101"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhir_101",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_102' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_102' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_102')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhir_102"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhir_102",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_103' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_103' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_103')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhir_103"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhir_103",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_104' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_104' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_104')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhir_104"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhir_104",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data

