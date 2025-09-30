
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_start_ipc_s4_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_start_ipc_s4
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_start_ipc_s4'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_start_ipc_s4"
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
                    WHERE ("column" = 'ipc_session_4_after_today_s_session_i_will_be_referring_you_for' OR linkid = 'ipc_session_4_after_today_s_session_i_will_be_referring_you_for' OR short_name = 'ipc_session_4_after_today_s_session_i_will_be_referring_you_for')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_4_after_today_s_session_i_will_be_referring_you_for"
            END as "ipc_session_4_after_today_s_session_i_will_be_referring_you_for",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_4_do_you_remember_why_you_came_let_s_talk_about_how' OR linkid = 'ipc_session_4_do_you_remember_why_you_came_let_s_talk_about_how' OR short_name = 'ipc_session_4_do_you_remember_why_you_came_let_s_talk_about_how')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_4_do_you_remember_why_you_came_let_s_talk_about_how"
            END as "ipc_session_4_do_you_remember_why_you_came_let_s_talk_about_how",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_4_what_do_you_think_are_some_situations_that_may_tr' OR linkid = 'ipc_session_4_what_do_you_think_are_some_situations_that_may_tr' OR short_name = 'ipc_session_4_what_do_you_think_are_some_situations_that_may_tr')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_4_what_do_you_think_are_some_situations_that_may_tr"
            END as "ipc_session_4_what_do_you_think_are_some_situations_that_may_tr",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_4_what_do_you_think_are_some_situations_that_may__2' OR linkid = 'ipc_session_4_what_do_you_think_are_some_situations_that_may__2' OR short_name = 'ipc_session_4_what_do_you_think_are_some_situations_that_may__2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_4_what_do_you_think_are_some_situations_that_may__2"
            END as "ipc_session_4_what_do_you_think_are_some_situations_that_may__2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_4_i_am_going_to_provide_you_with_our_agency_s_conta' OR linkid = 'ipc_session_4_i_am_going_to_provide_you_with_our_agency_s_conta' OR short_name = 'ipc_session_4_i_am_going_to_provide_you_with_our_agency_s_conta')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_4_i_am_going_to_provide_you_with_our_agency_s_conta"
            END as "ipc_session_4_i_am_going_to_provide_you_with_our_agency_s_conta",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'discussion_with_supervisor_for_severe_mental_health_did_you_d_2' OR linkid = 'discussion_with_supervisor_for_severe_mental_health_did_you_d_2' OR short_name = 'discussion_with_supervisor_for_severe_mental_health_did_you_d_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "discussion_with_supervisor_for_severe_mental_health_did_you_d_2"
            END as "discussion_with_supervisor_for_severe_mental_health_did_you_d_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_118' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_118' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_118')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhir_118"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhir_118",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_4_how_does_it_make_you_feel_to_know_that_today_is_o' OR linkid = 'ipc_session_4_how_does_it_make_you_feel_to_know_that_today_is_o' OR short_name = 'ipc_session_4_how_does_it_make_you_feel_to_know_that_today_is_o')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_4_how_does_it_make_you_feel_to_know_that_today_is_o"
            END as "ipc_session_4_how_does_it_make_you_feel_to_know_that_today_is_o",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'discussion_with_supervisor_for_severe_mental_health_what_is_t_2' OR linkid = 'discussion_with_supervisor_for_severe_mental_health_what_is_t_2' OR short_name = 'discussion_with_supervisor_for_severe_mental_health_what_is_t_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "discussion_with_supervisor_for_severe_mental_health_what_is_t_2"
            END as "discussion_with_supervisor_for_severe_mental_health_what_is_t_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'survey_response_13' OR linkid = 'survey_response_13' OR short_name = 'survey_response_13')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "survey_response_13"
            END as "survey_response_13",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_4_let_s_talk_about_what_skills_you_learned_here_tha' OR linkid = 'ipc_session_4_let_s_talk_about_what_skills_you_learned_here_tha' OR short_name = 'ipc_session_4_let_s_talk_about_what_skills_you_learned_here_tha')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_4_let_s_talk_about_what_skills_you_learned_here_tha"
            END as "ipc_session_4_let_s_talk_about_what_skills_you_learned_here_tha",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_4_let_s_talk_about_what_skills_you_learned_here_t_2' OR linkid = 'ipc_session_4_let_s_talk_about_what_skills_you_learned_here_t_2' OR short_name = 'ipc_session_4_let_s_talk_about_what_skills_you_learned_here_t_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_4_let_s_talk_about_what_skills_you_learned_here_t_2"
            END as "ipc_session_4_let_s_talk_about_what_skills_you_learned_here_t_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_4_let_s_look_at_your_symptom_changes_what_were_your' OR linkid = 'ipc_session_4_let_s_look_at_your_symptom_changes_what_were_your' OR short_name = 'ipc_session_4_let_s_look_at_your_symptom_changes_what_were_your')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_4_let_s_look_at_your_symptom_changes_what_were_your"
            END as "ipc_session_4_let_s_look_at_your_symptom_changes_what_were_your",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mood_rating_ipc_session_4' OR linkid = 'mood_rating_ipc_session_4' OR short_name = 'mood_rating_ipc_session_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mood_rating_ipc_session_4"
            END as "mood_rating_ipc_session_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_4_how_do_you_feel_about_today_being_our_last_sessio' OR linkid = 'ipc_session_4_how_do_you_feel_about_today_being_our_last_sessio' OR short_name = 'ipc_session_4_how_do_you_feel_about_today_being_our_last_sessio')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_4_how_do_you_feel_about_today_being_our_last_sessio"
            END as "ipc_session_4_how_do_you_feel_about_today_being_our_last_sessio",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'discussion_with_supervisor_for_severe_mental_health_referral__2' OR linkid = 'discussion_with_supervisor_for_severe_mental_health_referral__2' OR short_name = 'discussion_with_supervisor_for_severe_mental_health_referral__2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "discussion_with_supervisor_for_severe_mental_health_referral__2"
            END as "discussion_with_supervisor_for_severe_mental_health_referral__2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_4_in_what_format_did_you_deliver_this_session_with_' OR linkid = 'ipc_session_4_in_what_format_did_you_deliver_this_session_with_' OR short_name = 'ipc_session_4_in_what_format_did_you_deliver_this_session_with_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_4_in_what_format_did_you_deliver_this_session_with_"
            END as "ipc_session_4_in_what_format_did_you_deliver_this_session_with_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_4_how_would_you_know_if_your_symptoms_are_coming_ba' OR linkid = 'ipc_session_4_how_would_you_know_if_your_symptoms_are_coming_ba' OR short_name = 'ipc_session_4_how_would_you_know_if_your_symptoms_are_coming_ba')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_4_how_would_you_know_if_your_symptoms_are_coming_ba"
            END as "ipc_session_4_how_would_you_know_if_your_symptoms_are_coming_ba",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_4_after_today_s_session_i_can_refer_you_for_clinica' OR linkid = 'ipc_session_4_after_today_s_session_i_can_refer_you_for_clinica' OR short_name = 'ipc_session_4_after_today_s_session_i_can_refer_you_for_clinica')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_4_after_today_s_session_i_can_refer_you_for_clinica"
            END as "ipc_session_4_after_today_s_session_i_can_refer_you_for_clinica",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_4_reason' OR linkid = 'ipc_session_4_reason' OR short_name = 'ipc_session_4_reason')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_4_reason"
            END as "ipc_session_4_reason",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_4_what_are_some_of_the_skills_you_learned_here_that' OR linkid = 'ipc_session_4_what_are_some_of_the_skills_you_learned_here_that' OR short_name = 'ipc_session_4_what_are_some_of_the_skills_you_learned_here_that')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_4_what_are_some_of_the_skills_you_learned_here_that"
            END as "ipc_session_4_what_are_some_of_the_skills_you_learned_here_that",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_119' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_119' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_119')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhir_119"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhir_119",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_120' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_120' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_120')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhir_120"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhir_120",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_4_please_select_how_you_would_like_to_proceed_with_' OR linkid = 'ipc_session_4_please_select_how_you_would_like_to_proceed_with_' OR short_name = 'ipc_session_4_please_select_how_you_would_like_to_proceed_with_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_4_please_select_how_you_would_like_to_proceed_with_"
            END as "ipc_session_4_please_select_how_you_would_like_to_proceed_with_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_4_what_do_you_think_you_still_need_to_work_on_regar' OR linkid = 'ipc_session_4_what_do_you_think_you_still_need_to_work_on_regar' OR short_name = 'ipc_session_4_what_do_you_think_you_still_need_to_work_on_regar')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_4_what_do_you_think_you_still_need_to_work_on_regar"
            END as "ipc_session_4_what_do_you_think_you_still_need_to_work_on_regar",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_4_what_we_know_is_that_symptoms_can_come_back_in_th' OR linkid = 'ipc_session_4_what_we_know_is_that_symptoms_can_come_back_in_th' OR short_name = 'ipc_session_4_what_we_know_is_that_symptoms_can_come_back_in_th')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_4_what_we_know_is_that_symptoms_can_come_back_in_th"
            END as "ipc_session_4_what_we_know_is_that_symptoms_can_come_back_in_th",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_4_what_do_you_think_you_still_need_to_work_on_reg_2' OR linkid = 'ipc_session_4_what_do_you_think_you_still_need_to_work_on_reg_2' OR short_name = 'ipc_session_4_what_do_you_think_you_still_need_to_work_on_reg_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_4_what_do_you_think_you_still_need_to_work_on_reg_2"
            END as "ipc_session_4_what_do_you_think_you_still_need_to_work_on_reg_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_2_did_you_complete_the_required_assessment_s_and__4' OR linkid = 'ipc_session_2_did_you_complete_the_required_assessment_s_and__4' OR short_name = 'ipc_session_2_did_you_complete_the_required_assessment_s_and__4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_2_did_you_complete_the_required_assessment_s_and__4"
            END as "ipc_session_2_did_you_complete_the_required_assessment_s_and__4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'survey_response_14' OR linkid = 'survey_response_14' OR short_name = 'survey_response_14')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "survey_response_14"
            END as "survey_response_14",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_4_total_score' OR linkid = 'ipc_session_4_total_score' OR short_name = 'ipc_session_4_total_score')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_4_total_score"
            END as "ipc_session_4_total_score",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_4_what_are_some_of_the_skills_you_learned_here_th_2' OR linkid = 'ipc_session_4_what_are_some_of_the_skills_you_learned_here_th_2' OR short_name = 'ipc_session_4_what_are_some_of_the_skills_you_learned_here_th_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_4_what_are_some_of_the_skills_you_learned_here_th_2"
            END as "ipc_session_4_what_are_some_of_the_skills_you_learned_here_th_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_4_let_s_look_at_your_symptom_changes_what_were_yo_2' OR linkid = 'ipc_session_4_let_s_look_at_your_symptom_changes_what_were_yo_2' OR short_name = 'ipc_session_4_let_s_look_at_your_symptom_changes_what_were_yo_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_4_let_s_look_at_your_symptom_changes_what_were_yo_2"
            END as "ipc_session_4_let_s_look_at_your_symptom_changes_what_were_yo_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_121' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_121' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhir_121')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhir_121"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhir_121",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_4_do_you_remember_why_you_came_let_s_talk_about_h_2' OR linkid = 'ipc_session_4_do_you_remember_why_you_came_let_s_talk_about_h_2' OR short_name = 'ipc_session_4_do_you_remember_why_you_came_let_s_talk_about_h_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_4_do_you_remember_why_you_came_let_s_talk_about_h_2"
            END as "ipc_session_4_do_you_remember_why_you_came_let_s_talk_about_h_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'survey_response_15' OR linkid = 'survey_response_15' OR short_name = 'survey_response_15')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "survey_response_15"
            END as "survey_response_15",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_4_phq_9_total' OR linkid = 'ipc_session_4_phq_9_total' OR short_name = 'ipc_session_4_phq_9_total')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_4_phq_9_total"
            END as "ipc_session_4_phq_9_total",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_4_gad_7_total' OR linkid = 'ipc_session_4_gad_7_total' OR short_name = 'ipc_session_4_gad_7_total')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_4_gad_7_total"
            END as "ipc_session_4_gad_7_total",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_4_ptsd_total' OR linkid = 'ipc_session_4_ptsd_total' OR short_name = 'ipc_session_4_ptsd_total')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_4_ptsd_total"
            END as "ipc_session_4_ptsd_total",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being__8' OR linkid = 'mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being__8' OR short_name = 'mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being__8')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being__8"
            END as "mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being__8",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'discussion_with_supervisor_for_severe_mental_health_superviso_3' OR linkid = 'discussion_with_supervisor_for_severe_mental_health_superviso_3' OR short_name = 'discussion_with_supervisor_for_severe_mental_health_superviso_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "discussion_with_supervisor_for_severe_mental_health_superviso_3"
            END as "discussion_with_supervisor_for_severe_mental_health_superviso_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_4_details' OR linkid = 'ipc_session_4_details' OR short_name = 'ipc_session_4_details')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_4_details"
            END as "ipc_session_4_details",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_4_is_the_client_connected_to_clinical_care' OR linkid = 'ipc_session_4_is_the_client_connected_to_clinical_care' OR short_name = 'ipc_session_4_is_the_client_connected_to_clinical_care')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_4_is_the_client_connected_to_clinical_care"
            END as "ipc_session_4_is_the_client_connected_to_clinical_care",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data


  );