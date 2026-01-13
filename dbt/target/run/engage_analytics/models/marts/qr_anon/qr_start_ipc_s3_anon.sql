
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_start_ipc_s3_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_start_ipc_s3
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_start_ipc_s3'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_start_ipc_s3"
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
                    WHERE ("column" = 'decision_analysis_example_guiding_prompts_not_all_questions_n_5' OR linkid = 'decision_analysis_example_guiding_prompts_not_all_questions_n_5' OR short_name = 'decision_analysis_example_guiding_prompts_not_all_questions_n_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "decision_analysis_example_guiding_prompts_not_all_questions_n_5"
            END as "decision_analysis_example_guiding_prompts_not_all_questions_n_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_2_phq_9_total_meaning_2' OR linkid = 'ipc_session_2_phq_9_total_meaning_2' OR short_name = 'ipc_session_2_phq_9_total_meaning_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_2_phq_9_total_meaning_2"
            END as "ipc_session_2_phq_9_total_meaning_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'interpersonal_skills_building_example_skills_5' OR linkid = 'interpersonal_skills_building_example_skills_5' OR short_name = 'interpersonal_skills_building_example_skills_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "interpersonal_skills_building_example_skills_5"
            END as "interpersonal_skills_building_example_skills_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_2_gad_7_total_meaning_2' OR linkid = 'ipc_session_2_gad_7_total_meaning_2' OR short_name = 'ipc_session_2_gad_7_total_meaning_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_2_gad_7_total_meaning_2"
            END as "ipc_session_2_gad_7_total_meaning_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'role_play_example_guiding_prompts_not_all_questions_need_to_b_5' OR linkid = 'role_play_example_guiding_prompts_not_all_questions_need_to_b_5' OR short_name = 'role_play_example_guiding_prompts_not_all_questions_need_to_b_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "role_play_example_guiding_prompts_not_all_questions_need_to_b_5"
            END as "role_play_example_guiding_prompts_not_all_questions_need_to_b_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'renegotiation' OR linkid = 'renegotiation' OR short_name = 'renegotiation')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "renegotiation"
            END as "renegotiation",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'communication_analysis_example_guiding_prompts_not_all_questi_5' OR linkid = 'communication_analysis_example_guiding_prompts_not_all_questi_5' OR short_name = 'communication_analysis_example_guiding_prompts_not_all_questi_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "communication_analysis_example_guiding_prompts_not_all_questi_5"
            END as "communication_analysis_example_guiding_prompts_not_all_questi_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'explore_the_problem_questions_asked_in_session_2_2' OR linkid = 'explore_the_problem_questions_asked_in_session_2_2' OR short_name = 'explore_the_problem_questions_asked_in_session_2_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "explore_the_problem_questions_asked_in_session_2_2"
            END as "explore_the_problem_questions_asked_in_session_2_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'identified_stage_of_disagreement_stage_identified_in_session_2' OR linkid = 'identified_stage_of_disagreement_stage_identified_in_session_2' OR short_name = 'identified_stage_of_disagreement_stage_identified_in_session_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "identified_stage_of_disagreement_stage_identified_in_session_2"
            END as "identified_stage_of_disagreement_stage_identified_in_session_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'role_play_example_guiding_prompts_not_all_questions_need_to_b_6' OR linkid = 'role_play_example_guiding_prompts_not_all_questions_need_to_b_6' OR short_name = 'role_play_example_guiding_prompts_not_all_questions_need_to_b_6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "role_play_example_guiding_prompts_not_all_questions_need_to_b_6"
            END as "role_play_example_guiding_prompts_not_all_questions_need_to_b_6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mood_rating_ipc_session_3_total_score_2' OR linkid = 'mood_rating_ipc_session_3_total_score_2' OR short_name = 'mood_rating_ipc_session_3_total_score_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mood_rating_ipc_session_3_total_score_2"
            END as "mood_rating_ipc_session_3_total_score_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'impasse_questions_asked_in_session_2_2' OR linkid = 'impasse_questions_asked_in_session_2_2' OR short_name = 'impasse_questions_asked_in_session_2_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "impasse_questions_asked_in_session_2_2"
            END as "impasse_questions_asked_in_session_2_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'feelings_about_change_questions_asked_in_session_2_2' OR linkid = 'feelings_about_change_questions_asked_in_session_2_2' OR short_name = 'feelings_about_change_questions_asked_in_session_2_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "feelings_about_change_questions_asked_in_session_2_2"
            END as "feelings_about_change_questions_asked_in_session_2_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'explore_new_relationships_and_re_establish_old_relationships__2' OR linkid = 'explore_new_relationships_and_re_establish_old_relationships__2' OR short_name = 'explore_new_relationships_and_re_establish_old_relationships__2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "explore_new_relationships_and_re_establish_old_relationships__2"
            END as "explore_new_relationships_and_re_establish_old_relationships__2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'decision_analysis_5' OR linkid = 'decision_analysis_5' OR short_name = 'decision_analysis_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "decision_analysis_5"
            END as "decision_analysis_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'role_play_example_guiding_prompts_not_all_questions_need_to_b_7' OR linkid = 'role_play_example_guiding_prompts_not_all_questions_need_to_b_7' OR short_name = 'role_play_example_guiding_prompts_not_all_questions_need_to_b_7')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "role_play_example_guiding_prompts_not_all_questions_need_to_b_7"
            END as "role_play_example_guiding_prompts_not_all_questions_need_to_b_7",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_105' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_105' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_105')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhir_105"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhir_105",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'work_at_home_example_guiding_prompts_not_all_questions_need_t_5' OR linkid = 'work_at_home_example_guiding_prompts_not_all_questions_need_t_5' OR short_name = 'work_at_home_example_guiding_prompts_not_all_questions_need_t_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "work_at_home_example_guiding_prompts_not_all_questions_need_t_5"
            END as "work_at_home_example_guiding_prompts_not_all_questions_need_t_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'discuss_the_relationship' OR linkid = 'discuss_the_relationship' OR short_name = 'discuss_the_relationship')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "discuss_the_relationship"
            END as "discuss_the_relationship",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'assess_for_violence' OR linkid = 'assess_for_violence' OR short_name = 'assess_for_violence')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "assess_for_violence"
            END as "assess_for_violence",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_2_6' OR linkid = 'ipc_session_2_6' OR short_name = 'ipc_session_2_6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_2_6"
            END as "ipc_session_2_6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'survey_response_11' OR linkid = 'survey_response_11' OR short_name = 'survey_response_11')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "survey_response_11"
            END as "survey_response_11",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'decision_analysis_6' OR linkid = 'decision_analysis_6' OR short_name = 'decision_analysis_6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "decision_analysis_6"
            END as "decision_analysis_6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_2_7' OR linkid = 'ipc_session_2_7' OR short_name = 'ipc_session_2_7')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_2_7"
            END as "ipc_session_2_7",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'communication_analysis_example_guiding_prompts_not_all_questi_6' OR linkid = 'communication_analysis_example_guiding_prompts_not_all_questi_6' OR short_name = 'communication_analysis_example_guiding_prompts_not_all_questi_6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "communication_analysis_example_guiding_prompts_not_all_questi_6"
            END as "communication_analysis_example_guiding_prompts_not_all_questi_6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'role_play_example_guiding_prompts_not_all_questions_need_to_b_8' OR linkid = 'role_play_example_guiding_prompts_not_all_questions_need_to_b_8' OR short_name = 'role_play_example_guiding_prompts_not_all_questions_need_to_b_8')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "role_play_example_guiding_prompts_not_all_questions_need_to_b_8"
            END as "role_play_example_guiding_prompts_not_all_questions_need_to_b_8",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mood_rating_ipc_session_3' OR linkid = 'mood_rating_ipc_session_3' OR short_name = 'mood_rating_ipc_session_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mood_rating_ipc_session_3"
            END as "mood_rating_ipc_session_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'communication_analysis_example_guiding_prompts_not_all_questi_7' OR linkid = 'communication_analysis_example_guiding_prompts_not_all_questi_7' OR short_name = 'communication_analysis_example_guiding_prompts_not_all_questi_7')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "communication_analysis_example_guiding_prompts_not_all_questi_7"
            END as "communication_analysis_example_guiding_prompts_not_all_questi_7",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'work_at_home_example_guiding_prompts_not_all_questions_need_t_6' OR linkid = 'work_at_home_example_guiding_prompts_not_all_questions_need_t_6' OR short_name = 'work_at_home_example_guiding_prompts_not_all_questions_need_t_6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "work_at_home_example_guiding_prompts_not_all_questions_need_t_6"
            END as "work_at_home_example_guiding_prompts_not_all_questions_need_t_6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'the_old_role_questions_asked_in_session_2_2' OR linkid = 'the_old_role_questions_asked_in_session_2_2' OR short_name = 'the_old_role_questions_asked_in_session_2_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "the_old_role_questions_asked_in_session_2_2"
            END as "the_old_role_questions_asked_in_session_2_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'discuss_close_relationships_in_the_past' OR linkid = 'discuss_close_relationships_in_the_past' OR short_name = 'discuss_close_relationships_in_the_past')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "discuss_close_relationships_in_the_past"
            END as "discuss_close_relationships_in_the_past",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_2_would_you_prefer_to_focus_on_this_new_problem_a_3' OR linkid = 'ipc_session_2_would_you_prefer_to_focus_on_this_new_problem_a_3' OR short_name = 'ipc_session_2_would_you_prefer_to_focus_on_this_new_problem_a_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_2_would_you_prefer_to_focus_on_this_new_problem_a_3"
            END as "ipc_session_2_would_you_prefer_to_focus_on_this_new_problem_a_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'disagreements' OR linkid = 'disagreements' OR short_name = 'disagreements')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "disagreements"
            END as "disagreements",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'the_new_role_questions_asked_in_session_2_2' OR linkid = 'the_new_role_questions_asked_in_session_2_2' OR short_name = 'the_new_role_questions_asked_in_session_2_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "the_new_role_questions_asked_in_session_2_2"
            END as "the_new_role_questions_asked_in_session_2_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'discuss_the_relationship_questions_asked_in_session_2_2' OR linkid = 'discuss_the_relationship_questions_asked_in_session_2_2' OR short_name = 'discuss_the_relationship_questions_asked_in_session_2_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "discuss_the_relationship_questions_asked_in_session_2_2"
            END as "discuss_the_relationship_questions_asked_in_session_2_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'communication_analysis_example_guiding_prompts_not_all_questi_8' OR linkid = 'communication_analysis_example_guiding_prompts_not_all_questi_8' OR short_name = 'communication_analysis_example_guiding_prompts_not_all_questi_8')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "communication_analysis_example_guiding_prompts_not_all_questi_8"
            END as "communication_analysis_example_guiding_prompts_not_all_questi_8",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'renegotiation_questions_asked_in_session_2_2' OR linkid = 'renegotiation_questions_asked_in_session_2_2' OR short_name = 'renegotiation_questions_asked_in_session_2_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "renegotiation_questions_asked_in_session_2_2"
            END as "renegotiation_questions_asked_in_session_2_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'explore_the_problem' OR linkid = 'explore_the_problem' OR short_name = 'explore_the_problem')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "explore_the_problem"
            END as "explore_the_problem",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_2_8' OR linkid = 'ipc_session_2_8' OR short_name = 'ipc_session_2_8')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_2_8"
            END as "ipc_session_2_8",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'assess_for_violence_did_you_ever_hit_kick_push_or_slap_the_pe_2' OR linkid = 'assess_for_violence_did_you_ever_hit_kick_push_or_slap_the_pe_2' OR short_name = 'assess_for_violence_did_you_ever_hit_kick_push_or_slap_the_pe_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "assess_for_violence_did_you_ever_hit_kick_push_or_slap_the_pe_2"
            END as "assess_for_violence_did_you_ever_hit_kick_push_or_slap_the_pe_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'survey_response_12' OR linkid = 'survey_response_12' OR short_name = 'survey_response_12')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "survey_response_12"
            END as "survey_response_12",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'decision_analysis_example_guiding_prompts_not_all_questions_n_6' OR linkid = 'decision_analysis_example_guiding_prompts_not_all_questions_n_6' OR short_name = 'decision_analysis_example_guiding_prompts_not_all_questions_n_6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "decision_analysis_example_guiding_prompts_not_all_questions_n_6"
            END as "decision_analysis_example_guiding_prompts_not_all_questions_n_6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'the_new_role' OR linkid = 'the_new_role' OR short_name = 'the_new_role')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "the_new_role"
            END as "the_new_role",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'increase_activities_with_others' OR linkid = 'increase_activities_with_others' OR short_name = 'increase_activities_with_others')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "increase_activities_with_others"
            END as "increase_activities_with_others",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'work_at_home_example_guiding_prompts_not_all_questions_need_t_7' OR linkid = 'work_at_home_example_guiding_prompts_not_all_questions_need_t_7' OR short_name = 'work_at_home_example_guiding_prompts_not_all_questions_need_t_7')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "work_at_home_example_guiding_prompts_not_all_questions_need_t_7"
            END as "work_at_home_example_guiding_prompts_not_all_questions_need_t_7",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'explore_new_relationships_and_re_establish_old_relationships__3' OR linkid = 'explore_new_relationships_and_re_establish_old_relationships__3' OR short_name = 'explore_new_relationships_and_re_establish_old_relationships__3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "explore_new_relationships_and_re_establish_old_relationships__3"
            END as "explore_new_relationships_and_re_establish_old_relationships__3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'explore_difficulties_in_relationships' OR linkid = 'explore_difficulties_in_relationships' OR short_name = 'explore_difficulties_in_relationships')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "explore_difficulties_in_relationships"
            END as "explore_difficulties_in_relationships",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'decision_analysis_example_guiding_prompts_not_all_questions_n_7' OR linkid = 'decision_analysis_example_guiding_prompts_not_all_questions_n_7' OR short_name = 'decision_analysis_example_guiding_prompts_not_all_questions_n_7')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "decision_analysis_example_guiding_prompts_not_all_questions_n_7"
            END as "decision_analysis_example_guiding_prompts_not_all_questions_n_7",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'discuss_work_at_home_work_at_home_you_assigned_to_the_client__2' OR linkid = 'discuss_work_at_home_work_at_home_you_assigned_to_the_client__2' OR short_name = 'discuss_work_at_home_work_at_home_you_assigned_to_the_client__2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "discuss_work_at_home_work_at_home_you_assigned_to_the_client__2"
            END as "discuss_work_at_home_work_at_home_you_assigned_to_the_client__2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'go_to_the_interventions_tab_and_select_the_suicide_prevention_2' OR linkid = 'go_to_the_interventions_tab_and_select_the_suicide_prevention_2' OR short_name = 'go_to_the_interventions_tab_and_select_the_suicide_prevention_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "go_to_the_interventions_tab_and_select_the_suicide_prevention_2"
            END as "go_to_the_interventions_tab_and_select_the_suicide_prevention_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'explore_difficulties_in_relationships_questions_asked_in_sess_2' OR linkid = 'explore_difficulties_in_relationships_questions_asked_in_sess_2' OR short_name = 'explore_difficulties_in_relationships_questions_asked_in_sess_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "explore_difficulties_in_relationships_questions_asked_in_sess_2"
            END as "explore_difficulties_in_relationships_questions_asked_in_sess_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'the_old_role' OR linkid = 'the_old_role' OR short_name = 'the_old_role')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "the_old_role"
            END as "the_old_role",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'disagreements_questions_asked_in_session_2_2' OR linkid = 'disagreements_questions_asked_in_session_2_2' OR short_name = 'disagreements_questions_asked_in_session_2_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "disagreements_questions_asked_in_session_2_2"
            END as "disagreements_questions_asked_in_session_2_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'decision_analysis_example_guiding_prompts_not_all_questions_n_8' OR linkid = 'decision_analysis_example_guiding_prompts_not_all_questions_n_8' OR short_name = 'decision_analysis_example_guiding_prompts_not_all_questions_n_8')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "decision_analysis_example_guiding_prompts_not_all_questions_n_8"
            END as "decision_analysis_example_guiding_prompts_not_all_questions_n_8",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_106' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_106' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_106')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhir_106"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhir_106",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_107' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_107' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_107')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhir_107"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhir_107",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_108' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_108' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_108')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhir_108"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhir_108",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'guiding_prompts_for_stages_of_disagreements_2' OR linkid = 'guiding_prompts_for_stages_of_disagreements_2' OR short_name = 'guiding_prompts_for_stages_of_disagreements_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "guiding_prompts_for_stages_of_disagreements_2"
            END as "guiding_prompts_for_stages_of_disagreements_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'dissolution_questions_asked_in_session_2_2' OR linkid = 'dissolution_questions_asked_in_session_2_2' OR short_name = 'dissolution_questions_asked_in_session_2_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "dissolution_questions_asked_in_session_2_2"
            END as "dissolution_questions_asked_in_session_2_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_3_in_what_format_did_you_deliver_this_session_with_' OR linkid = 'ipc_session_3_in_what_format_did_you_deliver_this_session_with_' OR short_name = 'ipc_session_3_in_what_format_did_you_deliver_this_session_with_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_3_in_what_format_did_you_deliver_this_session_with_"
            END as "ipc_session_3_in_what_format_did_you_deliver_this_session_with_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'impasse' OR linkid = 'impasse' OR short_name = 'impasse')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "impasse"
            END as "impasse",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'interpersonal_skills_building_example_skills_6' OR linkid = 'interpersonal_skills_building_example_skills_6' OR short_name = 'interpersonal_skills_building_example_skills_6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "interpersonal_skills_building_example_skills_6"
            END as "interpersonal_skills_building_example_skills_6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_2_did_you_complete_the_required_assessment_s_and__3' OR linkid = 'ipc_session_2_did_you_complete_the_required_assessment_s_and__3' OR short_name = 'ipc_session_2_did_you_complete_the_required_assessment_s_and__3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_2_did_you_complete_the_required_assessment_s_and__3"
            END as "ipc_session_2_did_you_complete_the_required_assessment_s_and__3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_age_12' OR linkid = 'add_family_member_registration_calculated_age_12' OR short_name = 'add_family_member_registration_calculated_age_12')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_age_12"
            END as "add_family_member_registration_calculated_age_12",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'assess_for_violence_has_anything_changed_since_the_last_time_we' OR linkid = 'assess_for_violence_has_anything_changed_since_the_last_time_we' OR short_name = 'assess_for_violence_has_anything_changed_since_the_last_time_we')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "assess_for_violence_has_anything_changed_since_the_last_time_we"
            END as "assess_for_violence_has_anything_changed_since_the_last_time_we",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'discuss_the_death_questions_asked_in_session_2_2' OR linkid = 'discuss_the_death_questions_asked_in_session_2_2' OR short_name = 'discuss_the_death_questions_asked_in_session_2_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "discuss_the_death_questions_asked_in_session_2_2"
            END as "discuss_the_death_questions_asked_in_session_2_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'decision_analysis_7' OR linkid = 'decision_analysis_7' OR short_name = 'decision_analysis_7')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "decision_analysis_7"
            END as "decision_analysis_7",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'increase_activities_with_others_questions_asked_in_session_2_2' OR linkid = 'increase_activities_with_others_questions_asked_in_session_2_2' OR short_name = 'increase_activities_with_others_questions_asked_in_session_2_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "increase_activities_with_others_questions_asked_in_session_2_2"
            END as "increase_activities_with_others_questions_asked_in_session_2_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_2_9' OR linkid = 'ipc_session_2_9' OR short_name = 'ipc_session_2_9')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_2_9"
            END as "ipc_session_2_9",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_month_12' OR linkid = 'add_family_member_registration_calculated_month_12' OR short_name = 'add_family_member_registration_calculated_month_12')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_month_12"
            END as "add_family_member_registration_calculated_month_12",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_year_12' OR linkid = 'add_family_member_registration_calculated_year_12' OR short_name = 'add_family_member_registration_calculated_year_12')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_year_12"
            END as "add_family_member_registration_calculated_year_12",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_date_of_birth_12' OR linkid = 'add_family_member_registration_date_of_birth_12' OR short_name = 'add_family_member_registration_date_of_birth_12')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_date_of_birth_12"
            END as "add_family_member_registration_date_of_birth_12",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_3_phq_9_total' OR linkid = 'ipc_session_3_phq_9_total' OR short_name = 'ipc_session_3_phq_9_total')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_3_phq_9_total"
            END as "ipc_session_3_phq_9_total",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_3_gad_7_total' OR linkid = 'ipc_session_3_gad_7_total' OR short_name = 'ipc_session_3_gad_7_total')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_3_gad_7_total"
            END as "ipc_session_3_gad_7_total",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_3_ptsd_total' OR linkid = 'ipc_session_3_ptsd_total' OR short_name = 'ipc_session_3_ptsd_total')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_3_ptsd_total"
            END as "ipc_session_3_ptsd_total",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_3_ptsd_total_meaning' OR linkid = 'ipc_session_3_ptsd_total_meaning' OR short_name = 'ipc_session_3_ptsd_total_meaning')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_3_ptsd_total_meaning"
            END as "ipc_session_3_ptsd_total_meaning",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'identified_stage_of_disagreement_has_there_been_a_change_in_t_2' OR linkid = 'identified_stage_of_disagreement_has_there_been_a_change_in_t_2' OR short_name = 'identified_stage_of_disagreement_has_there_been_a_change_in_t_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "identified_stage_of_disagreement_has_there_been_a_change_in_t_2"
            END as "identified_stage_of_disagreement_has_there_been_a_change_in_t_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'discuss_close_relationships_in_the_past_questions_asked_in_se_2' OR linkid = 'discuss_close_relationships_in_the_past_questions_asked_in_se_2' OR short_name = 'discuss_close_relationships_in_the_past_questions_asked_in_se_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "discuss_close_relationships_in_the_past_questions_asked_in_se_2"
            END as "discuss_close_relationships_in_the_past_questions_asked_in_se_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'dissolution' OR linkid = 'dissolution' OR short_name = 'dissolution')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "dissolution"
            END as "dissolution",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'interpersonal_skills_building_example_skills_7' OR linkid = 'interpersonal_skills_building_example_skills_7' OR short_name = 'interpersonal_skills_building_example_skills_7')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "interpersonal_skills_building_example_skills_7"
            END as "interpersonal_skills_building_example_skills_7",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_109' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_109' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_109')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhir_109"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhir_109",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'decision_analysis_8' OR linkid = 'decision_analysis_8' OR short_name = 'decision_analysis_8')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "decision_analysis_8"
            END as "decision_analysis_8",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'discuss_the_death' OR linkid = 'discuss_the_death' OR short_name = 'discuss_the_death')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "discuss_the_death"
            END as "discuss_the_death",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'work_at_home_example_guiding_prompts_not_all_questions_need_t_8' OR linkid = 'work_at_home_example_guiding_prompts_not_all_questions_need_t_8' OR short_name = 'work_at_home_example_guiding_prompts_not_all_questions_need_t_8')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "work_at_home_example_guiding_prompts_not_all_questions_need_t_8"
            END as "work_at_home_example_guiding_prompts_not_all_questions_need_t_8",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'identify_social_settings_questions_asked_in_session_2_2' OR linkid = 'identify_social_settings_questions_asked_in_session_2_2' OR short_name = 'identify_social_settings_questions_asked_in_session_2_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "identify_social_settings_questions_asked_in_session_2_2"
            END as "identify_social_settings_questions_asked_in_session_2_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'interpersonal_skills_building_example_skills_8' OR linkid = 'interpersonal_skills_building_example_skills_8' OR short_name = 'interpersonal_skills_building_example_skills_8')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "interpersonal_skills_building_example_skills_8"
            END as "interpersonal_skills_building_example_skills_8",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_110' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_110' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_110')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhir_110"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhir_110",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being__7' OR linkid = 'mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being__7' OR short_name = 'mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being__7')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being__7"
            END as "mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being__7",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'guiding_prompts_for_disagreements_2' OR linkid = 'guiding_prompts_for_disagreements_2' OR short_name = 'guiding_prompts_for_disagreements_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "guiding_prompts_for_disagreements_2"
            END as "guiding_prompts_for_disagreements_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'feelings_about_change' OR linkid = 'feelings_about_change' OR short_name = 'feelings_about_change')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "feelings_about_change"
            END as "feelings_about_change",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_111' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_111' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_111')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhir_111"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhir_111",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_112' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_112' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_112')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhir_112"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhir_112",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_113' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_113' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_113')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhir_113"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhir_113",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_114' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_114' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_114')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhir_114"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhir_114",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_115' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_115' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_115')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhir_115"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhir_115",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_116' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_116' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_116')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhir_116"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhir_116",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_117' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_117' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_117')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhir_117"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhir_117",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data


  );