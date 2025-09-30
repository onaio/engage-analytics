
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_sbirt_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_sbirt
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_sbirt'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_sbirt"
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
                    WHERE ("column" = 'sbirt_please_select_one_substance_you_would_like_to_discuss_fur' OR linkid = 'sbirt_please_select_one_substance_you_would_like_to_discuss_fur' OR short_name = 'sbirt_please_select_one_substance_you_would_like_to_discuss_fur')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_please_select_one_substance_you_would_like_to_discuss_fur"
            END as "sbirt_please_select_one_substance_you_would_like_to_discuss_fur",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_on_average_how_many_days_per_week_do_you_use_methamphetam' OR linkid = 'sbirt_on_average_how_many_days_per_week_do_you_use_methamphetam' OR short_name = 'sbirt_on_average_how_many_days_per_week_do_you_use_methamphetam')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_on_average_how_many_days_per_week_do_you_use_methamphetam"
            END as "sbirt_on_average_how_many_days_per_week_do_you_use_methamphetam",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_cocaine_coke_crack_etc' OR linkid = 'sbirt_cocaine_coke_crack_etc' OR short_name = 'sbirt_cocaine_coke_crack_etc')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_cocaine_coke_crack_etc"
            END as "sbirt_cocaine_coke_crack_etc",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'survey_response_2' OR linkid = 'survey_response_2' OR short_name = 'survey_response_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "survey_response_2"
            END as "survey_response_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_in_the_past_three_months_how_often_have_you_failed_to_do_' OR linkid = 'sbirt_in_the_past_three_months_how_often_have_you_failed_to_do_' OR short_name = 'sbirt_in_the_past_three_months_how_often_have_you_failed_to_do_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_in_the_past_three_months_how_often_have_you_failed_to_do_"
            END as "sbirt_in_the_past_three_months_how_often_have_you_failed_to_do_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_i_would_like_to_offer_an_opportunity_to_start_a_conversat' OR linkid = 'sbirt_i_would_like_to_offer_an_opportunity_to_start_a_conversat' OR short_name = 'sbirt_i_would_like_to_offer_an_opportunity_to_start_a_conversat')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_i_would_like_to_offer_an_opportunity_to_start_a_conversat"
            END as "sbirt_i_would_like_to_offer_an_opportunity_to_start_a_conversat",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed_co' OR linkid = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed_co' OR short_name = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed_co')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed_co"
            END as "sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed_co",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_sto' OR linkid = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_sto' OR short_name = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_sto')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_sto"
            END as "sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_sto",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_2' OR linkid = 'sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_2' OR short_name = 'sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_2"
            END as "sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_on_average_how_many_days_per_week_do_you_drink_alcohol' OR linkid = 'sbirt_on_average_how_many_days_per_week_do_you_drink_alcohol' OR short_name = 'sbirt_on_average_how_many_days_per_week_do_you_drink_alcohol')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_on_average_how_many_days_per_week_do_you_drink_alcohol"
            END as "sbirt_on_average_how_many_days_per_week_do_you_drink_alcohol",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_during_the_past_three_months_how_often_have_you_had_a_str' OR linkid = 'sbirt_during_the_past_three_months_how_often_have_you_had_a_str' OR short_name = 'sbirt_during_the_past_three_months_how_often_have_you_had_a_str')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_during_the_past_three_months_how_often_have_you_had_a_str"
            END as "sbirt_during_the_past_three_months_how_often_have_you_had_a_str",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'survey_response_3' OR linkid = 'survey_response_3' OR short_name = 'survey_response_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "survey_response_3"
            END as "survey_response_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'survey_response_4' OR linkid = 'survey_response_4' OR short_name = 'survey_response_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "survey_response_4"
            END as "survey_response_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_2' OR linkid = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_2' OR short_name = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_2"
            END as "sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_during_the_past_three_months_how_often_have_you_had_a_s_2' OR linkid = 'sbirt_during_the_past_three_months_how_often_have_you_had_a_s_2' OR short_name = 'sbirt_during_the_past_three_months_how_often_have_you_had_a_s_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_during_the_past_three_months_how_often_have_you_had_a_s_2"
            END as "sbirt_during_the_past_three_months_how_often_have_you_had_a_s_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_during_the_past_three_months_how_often_have_you_had_a_s_3' OR linkid = 'sbirt_during_the_past_three_months_how_often_have_you_had_a_s_3' OR short_name = 'sbirt_during_the_past_three_months_how_often_have_you_had_a_s_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_during_the_past_three_months_how_often_have_you_had_a_s_3"
            END as "sbirt_during_the_past_three_months_how_often_have_you_had_a_s_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_i_want_to_use_no_more_than' OR linkid = 'sbirt_i_want_to_use_no_more_than' OR short_name = 'sbirt_i_want_to_use_no_more_than')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_i_want_to_use_no_more_than"
            END as "sbirt_i_want_to_use_no_more_than",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt' OR linkid = 'sbirt' OR short_name = 'sbirt')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt"
            END as "sbirt",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_what_service_was_the_client_referred_to' OR linkid = 'sbirt_what_service_was_the_client_referred_to' OR short_name = 'sbirt_what_service_was_the_client_referred_to')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_what_service_was_the_client_referred_to"
            END as "sbirt_what_service_was_the_client_referred_to",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_prescription_stimulants_just_for_the_feeling_more_than_pr' OR linkid = 'sbirt_prescription_stimulants_just_for_the_feeling_more_than_pr' OR short_name = 'sbirt_prescription_stimulants_just_for_the_feeling_more_than_pr')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_prescription_stimulants_just_for_the_feeling_more_than_pr"
            END as "sbirt_prescription_stimulants_just_for_the_feeling_more_than_pr",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_on_average_how_many_days_per_week_do_you_use_other_drugs' OR linkid = 'sbirt_on_average_how_many_days_per_week_do_you_use_other_drugs' OR short_name = 'sbirt_on_average_how_many_days_per_week_do_you_use_other_drugs')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_on_average_how_many_days_per_week_do_you_use_other_drugs"
            END as "sbirt_on_average_how_many_days_per_week_do_you_use_other_drugs",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_hallucinogens_lsd_acid_mushrooms_pcp_special_k_ecstasy_et' OR linkid = 'sbirt_hallucinogens_lsd_acid_mushrooms_pcp_special_k_ecstasy_et' OR short_name = 'sbirt_hallucinogens_lsd_acid_mushrooms_pcp_special_k_ecstasy_et')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_hallucinogens_lsd_acid_mushrooms_pcp_special_k_ecstasy_et"
            END as "sbirt_hallucinogens_lsd_acid_mushrooms_pcp_special_k_ecstasy_et",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_i_want_to_use_no_more_than_2' OR linkid = 'sbirt_i_want_to_use_no_more_than_2' OR short_name = 'sbirt_i_want_to_use_no_more_than_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_i_want_to_use_no_more_than_2"
            END as "sbirt_i_want_to_use_no_more_than_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_in_the_past_three_months_has_your_use_of_prescription_sti' OR linkid = 'sbirt_in_the_past_three_months_has_your_use_of_prescription_sti' OR short_name = 'sbirt_in_the_past_three_months_has_your_use_of_prescription_sti')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_in_the_past_three_months_has_your_use_of_prescription_sti"
            END as "sbirt_in_the_past_three_months_has_your_use_of_prescription_sti",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_would_you_be_interested_in_talking_and_learning_more_abou' OR linkid = 'sbirt_would_you_be_interested_in_talking_and_learning_more_abou' OR short_name = 'sbirt_would_you_be_interested_in_talking_and_learning_more_abou')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_would_you_be_interested_in_talking_and_learning_more_abou"
            END as "sbirt_would_you_be_interested_in_talking_and_learning_more_abou",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_are_you_interested_in_learning_more_about_what_alcohol_do' OR linkid = 'sbirt_are_you_interested_in_learning_more_about_what_alcohol_do' OR short_name = 'sbirt_are_you_interested_in_learning_more_about_what_alcohol_do')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_are_you_interested_in_learning_more_about_what_alcohol_do"
            END as "sbirt_are_you_interested_in_learning_more_about_what_alcohol_do",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_in_the_past_three_months_has_your_use_of_methamphetamine_' OR linkid = 'sbirt_in_the_past_three_months_has_your_use_of_methamphetamine_' OR short_name = 'sbirt_in_the_past_three_months_has_your_use_of_methamphetamine_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_in_the_past_three_months_has_your_use_of_methamphetamine_"
            END as "sbirt_in_the_past_three_months_has_your_use_of_methamphetamine_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_in_the_past_three_months_has_your_use_of_other_drugs_led_' OR linkid = 'sbirt_in_the_past_three_months_has_your_use_of_other_drugs_led_' OR short_name = 'sbirt_in_the_past_three_months_has_your_use_of_other_drugs_led_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_in_the_past_three_months_has_your_use_of_other_drugs_led_"
            END as "sbirt_in_the_past_three_months_has_your_use_of_other_drugs_led_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_in_the_past_three_months_how_often_have_you_used_cocaine' OR linkid = 'sbirt_in_the_past_three_months_how_often_have_you_used_cocaine' OR short_name = 'sbirt_in_the_past_three_months_how_often_have_you_used_cocaine')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_in_the_past_three_months_how_often_have_you_used_cocaine"
            END as "sbirt_in_the_past_three_months_how_often_have_you_used_cocaine",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_3' OR linkid = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_3' OR short_name = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_3"
            END as "sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__2' OR linkid = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__2' OR short_name = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__2"
            END as "sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_i_want_to_drink_no_more_than' OR linkid = 'sbirt_i_want_to_drink_no_more_than' OR short_name = 'sbirt_i_want_to_drink_no_more_than')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_i_want_to_drink_no_more_than"
            END as "sbirt_i_want_to_drink_no_more_than",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_2' OR linkid = 'sbirt_2' OR short_name = 'sbirt_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_2"
            END as "sbirt_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__3' OR linkid = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__3' OR short_name = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__3"
            END as "sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_inhalants_nitrous_glue_paint_thinner_poppers_whippets_etc' OR linkid = 'sbirt_inhalants_nitrous_glue_paint_thinner_poppers_whippets_etc' OR short_name = 'sbirt_inhalants_nitrous_glue_paint_thinner_poppers_whippets_etc')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_inhalants_nitrous_glue_paint_thinner_poppers_whippets_etc"
            END as "sbirt_inhalants_nitrous_glue_paint_thinner_poppers_whippets_etc",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_have_you_or_someone_else_been_injured_as_a_result_of_your' OR linkid = 'sbirt_have_you_or_someone_else_been_injured_as_a_result_of_your' OR short_name = 'sbirt_have_you_or_someone_else_been_injured_as_a_result_of_your')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_have_you_or_someone_else_been_injured_as_a_result_of_your"
            END as "sbirt_have_you_or_someone_else_been_injured_as_a_result_of_your",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__4' OR linkid = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__4' OR short_name = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__4"
            END as "sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_how_often_do_you_have_six_or_more_drinks_on_one_occasion' OR linkid = 'sbirt_how_often_do_you_have_six_or_more_drinks_on_one_occasion' OR short_name = 'sbirt_how_often_do_you_have_six_or_more_drinks_on_one_occasion')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_how_often_do_you_have_six_or_more_drinks_on_one_occasion"
            END as "sbirt_how_often_do_you_have_six_or_more_drinks_on_one_occasion",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_on_average_how_many_days_per_week_do_you_use_cannabis' OR linkid = 'sbirt_on_average_how_many_days_per_week_do_you_use_cannabis' OR short_name = 'sbirt_on_average_how_many_days_per_week_do_you_use_cannabis')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_on_average_how_many_days_per_week_do_you_use_cannabis"
            END as "sbirt_on_average_how_many_days_per_week_do_you_use_cannabis",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_in_the_past_three_months_has_your_use_of_inhalants_led_to' OR linkid = 'sbirt_in_the_past_three_months_has_your_use_of_inhalants_led_to' OR short_name = 'sbirt_in_the_past_three_months_has_your_use_of_inhalants_led_to')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_in_the_past_three_months_has_your_use_of_inhalants_led_to"
            END as "sbirt_in_the_past_three_months_has_your_use_of_inhalants_led_to",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_how_often_during_the_last_year_have_you_needed_a_first_dr' OR linkid = 'sbirt_how_often_during_the_last_year_have_you_needed_a_first_dr' OR short_name = 'sbirt_how_often_during_the_last_year_have_you_needed_a_first_dr')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_how_often_during_the_last_year_have_you_needed_a_first_dr"
            END as "sbirt_how_often_during_the_last_year_have_you_needed_a_first_dr",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_during_the_past_three_months_how_often_have_you_had_a_s_4' OR linkid = 'sbirt_during_the_past_three_months_how_often_have_you_had_a_s_4' OR short_name = 'sbirt_during_the_past_three_months_how_often_have_you_had_a_s_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_during_the_past_three_months_how_often_have_you_had_a_s_4"
            END as "sbirt_during_the_past_three_months_how_often_have_you_had_a_s_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_on_average_how_many_days_per_week_do_you_use_hallucinogen' OR linkid = 'sbirt_on_average_how_many_days_per_week_do_you_use_hallucinogen' OR short_name = 'sbirt_on_average_how_many_days_per_week_do_you_use_hallucinogen')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_on_average_how_many_days_per_week_do_you_use_hallucinogen"
            END as "sbirt_on_average_how_many_days_per_week_do_you_use_hallucinogen",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_what_are_your_health_goals_around_substance_use' OR linkid = 'sbirt_what_are_your_health_goals_around_substance_use' OR short_name = 'sbirt_what_are_your_health_goals_around_substance_use')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_what_are_your_health_goals_around_substance_use"
            END as "sbirt_what_are_your_health_goals_around_substance_use",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_3' OR linkid = 'sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_3' OR short_name = 'sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_3"
            END as "sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_have_you_ever_experienced_withdrawal_symptoms_felt_sick_w' OR linkid = 'sbirt_have_you_ever_experienced_withdrawal_symptoms_felt_sick_w' OR short_name = 'sbirt_have_you_ever_experienced_withdrawal_symptoms_felt_sick_w')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_have_you_ever_experienced_withdrawal_symptoms_felt_sick_w"
            END as "sbirt_have_you_ever_experienced_withdrawal_symptoms_felt_sick_w",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'survey_response_5' OR linkid = 'survey_response_5' OR short_name = 'survey_response_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "survey_response_5"
            END as "survey_response_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__5' OR linkid = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__5' OR short_name = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__5"
            END as "sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_on_average_how_many_days_per_week_do_you_use_sedatives' OR linkid = 'sbirt_on_average_how_many_days_per_week_do_you_use_sedatives' OR short_name = 'sbirt_on_average_how_many_days_per_week_do_you_use_sedatives')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_on_average_how_many_days_per_week_do_you_use_sedatives"
            END as "sbirt_on_average_how_many_days_per_week_do_you_use_sedatives",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_in_the_past_three_months_has_your_use_of_cannabis_led_to_' OR linkid = 'sbirt_in_the_past_three_months_has_your_use_of_cannabis_led_to_' OR short_name = 'sbirt_in_the_past_three_months_has_your_use_of_cannabis_led_to_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_in_the_past_three_months_has_your_use_of_cannabis_led_to_"
            END as "sbirt_in_the_past_three_months_has_your_use_of_cannabis_led_to_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_i_want_to_drink_no_more_than_2' OR linkid = 'sbirt_i_want_to_drink_no_more_than_2' OR short_name = 'sbirt_i_want_to_drink_no_more_than_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_i_want_to_drink_no_more_than_2"
            END as "sbirt_i_want_to_drink_no_more_than_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_during_the_past_three_months_how_often_have_you_had_a_s_5' OR linkid = 'sbirt_during_the_past_three_months_how_often_have_you_had_a_s_5' OR short_name = 'sbirt_during_the_past_three_months_how_often_have_you_had_a_s_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_during_the_past_three_months_how_often_have_you_had_a_s_5"
            END as "sbirt_during_the_past_three_months_how_often_have_you_had_a_s_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_on_average_how_many_days_per_week_do_you_use_street_opioi' OR linkid = 'sbirt_on_average_how_many_days_per_week_do_you_use_street_opioi' OR short_name = 'sbirt_on_average_how_many_days_per_week_do_you_use_street_opioi')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_on_average_how_many_days_per_week_do_you_use_street_opioi"
            END as "sbirt_on_average_how_many_days_per_week_do_you_use_street_opioi",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_would_you_like_to_make_some_health_goals_around_substance' OR linkid = 'sbirt_would_you_like_to_make_some_health_goals_around_substance' OR short_name = 'sbirt_would_you_like_to_make_some_health_goals_around_substance')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_would_you_like_to_make_some_health_goals_around_substance"
            END as "sbirt_would_you_like_to_make_some_health_goals_around_substance",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_have_you_had_blackouts_or_flashbacks_as_a_result_of_drug_' OR linkid = 'sbirt_have_you_had_blackouts_or_flashbacks_as_a_result_of_drug_' OR short_name = 'sbirt_have_you_had_blackouts_or_flashbacks_as_a_result_of_drug_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_have_you_had_blackouts_or_flashbacks_as_a_result_of_drug_"
            END as "sbirt_have_you_had_blackouts_or_flashbacks_as_a_result_of_drug_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_during_the_past_three_months_how_often_have_you_had_a_s_6' OR linkid = 'sbirt_during_the_past_three_months_how_often_have_you_had_a_s_6' OR short_name = 'sbirt_during_the_past_three_months_how_often_have_you_had_a_s_6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_during_the_past_three_months_how_often_have_you_had_a_s_6"
            END as "sbirt_during_the_past_three_months_how_often_have_you_had_a_s_6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_in_the_past_three_months_how_often_have_you_used_other_dr' OR linkid = 'sbirt_in_the_past_three_months_how_often_have_you_used_other_dr' OR short_name = 'sbirt_in_the_past_three_months_how_often_have_you_used_other_dr')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_in_the_past_three_months_how_often_have_you_used_other_dr"
            END as "sbirt_in_the_past_three_months_how_often_have_you_used_other_dr",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_did_the_client_accept_the_referral' OR linkid = 'sbirt_did_the_client_accept_the_referral' OR short_name = 'sbirt_did_the_client_accept_the_referral')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_did_the_client_accept_the_referral"
            END as "sbirt_did_the_client_accept_the_referral",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_4' OR linkid = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_4' OR short_name = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_4"
            END as "sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_during_the_past_three_months_how_often_have_you_had_a_s_7' OR linkid = 'sbirt_during_the_past_three_months_how_often_have_you_had_a_s_7' OR short_name = 'sbirt_during_the_past_three_months_how_often_have_you_had_a_s_7')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_during_the_past_three_months_how_often_have_you_had_a_s_7"
            END as "sbirt_during_the_past_three_months_how_often_have_you_had_a_s_7",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_how_often_during_the_last_year_have_you_had_a_feeling_of_' OR linkid = 'sbirt_how_often_during_the_last_year_have_you_had_a_feeling_of_' OR short_name = 'sbirt_how_often_during_the_last_year_have_you_had_a_feeling_of_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_how_often_during_the_last_year_have_you_had_a_feeling_of_"
            END as "sbirt_how_often_during_the_last_year_have_you_had_a_feeling_of_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_how_many_standard_drinks_containing_alcohol_do_you_have_o' OR linkid = 'sbirt_how_many_standard_drinks_containing_alcohol_do_you_have_o' OR short_name = 'sbirt_how_many_standard_drinks_containing_alcohol_do_you_have_o')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_how_many_standard_drinks_containing_alcohol_do_you_have_o"
            END as "sbirt_how_many_standard_drinks_containing_alcohol_do_you_have_o",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_4' OR linkid = 'sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_4' OR short_name = 'sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_4"
            END as "sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_how_often_during_the_last_year_have_you_been_unable_to_re' OR linkid = 'sbirt_how_often_during_the_last_year_have_you_been_unable_to_re' OR short_name = 'sbirt_how_often_during_the_last_year_have_you_been_unable_to_re')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_how_often_during_the_last_year_have_you_been_unable_to_re"
            END as "sbirt_how_often_during_the_last_year_have_you_been_unable_to_re",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_lets_start_by_discussing_the_pros_and_cons_of_changing_yo' OR linkid = 'sbirt_lets_start_by_discussing_the_pros_and_cons_of_changing_yo' OR short_name = 'sbirt_lets_start_by_discussing_the_pros_and_cons_of_changing_yo')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_lets_start_by_discussing_the_pros_and_cons_of_changing_yo"
            END as "sbirt_lets_start_by_discussing_the_pros_and_cons_of_changing_yo",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_cannabis_marijuana_pot_grass_hash_etc' OR linkid = 'sbirt_cannabis_marijuana_pot_grass_hash_etc' OR short_name = 'sbirt_cannabis_marijuana_pot_grass_hash_etc')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_cannabis_marijuana_pot_grass_hash_etc"
            END as "sbirt_cannabis_marijuana_pot_grass_hash_etc",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_on_average_how_many_days_per_week_do_you_use_inhalants' OR linkid = 'sbirt_on_average_how_many_days_per_week_do_you_use_inhalants' OR short_name = 'sbirt_on_average_how_many_days_per_week_do_you_use_inhalants')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_on_average_how_many_days_per_week_do_you_use_inhalants"
            END as "sbirt_on_average_how_many_days_per_week_do_you_use_inhalants",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_5' OR linkid = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_5' OR short_name = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_5"
            END as "sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__6' OR linkid = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__6' OR short_name = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__6"
            END as "sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_in_the_past_three_months_has_your_use_of_street_opioids_o' OR linkid = 'sbirt_in_the_past_three_months_has_your_use_of_street_opioids_o' OR short_name = 'sbirt_in_the_past_three_months_has_your_use_of_street_opioids_o')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_in_the_past_three_months_has_your_use_of_street_opioids_o"
            END as "sbirt_in_the_past_three_months_has_your_use_of_street_opioids_o",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_on_average_how_many_days_per_week_do_you_use_cocaine' OR linkid = 'sbirt_on_average_how_many_days_per_week_do_you_use_cocaine' OR short_name = 'sbirt_on_average_how_many_days_per_week_do_you_use_cocaine')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_on_average_how_many_days_per_week_do_you_use_cocaine"
            END as "sbirt_on_average_how_many_days_per_week_do_you_use_cocaine",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_6' OR linkid = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_6' OR short_name = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_6"
            END as "sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_how_often_do_you_have_a_drink_containing_alcohol' OR linkid = 'sbirt_how_often_do_you_have_a_drink_containing_alcohol' OR short_name = 'sbirt_how_often_do_you_have_a_drink_containing_alcohol')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_how_often_do_you_have_a_drink_containing_alcohol"
            END as "sbirt_how_often_do_you_have_a_drink_containing_alcohol",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__7' OR linkid = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__7' OR short_name = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__7')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__7"
            END as "sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__7",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__8' OR linkid = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__8' OR short_name = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__8')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__8"
            END as "sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__8",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_any_other_drugs_to_get_high' OR linkid = 'sbirt_any_other_drugs_to_get_high' OR short_name = 'sbirt_any_other_drugs_to_get_high')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_any_other_drugs_to_get_high"
            END as "sbirt_any_other_drugs_to_get_high",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_have_you_neglected_your_family_because_of_your_use_of_dru' OR linkid = 'sbirt_have_you_neglected_your_family_because_of_your_use_of_dru' OR short_name = 'sbirt_have_you_neglected_your_family_because_of_your_use_of_dru')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_have_you_neglected_your_family_because_of_your_use_of_dru"
            END as "sbirt_have_you_neglected_your_family_because_of_your_use_of_dru",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_street_opioids_heroin_opium_etc_or_prescription_opioids_j' OR linkid = 'sbirt_street_opioids_heroin_opium_etc_or_prescription_opioids_j' OR short_name = 'sbirt_street_opioids_heroin_opium_etc_or_prescription_opioids_j')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_street_opioids_heroin_opium_etc_or_prescription_opioids_j"
            END as "sbirt_street_opioids_heroin_opium_etc_or_prescription_opioids_j",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'survey_response_6' OR linkid = 'survey_response_6' OR short_name = 'survey_response_6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "survey_response_6"
            END as "survey_response_6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_in_the_past_three_months_has_your_use_of_hallucinogens_le' OR linkid = 'sbirt_in_the_past_three_months_has_your_use_of_hallucinogens_le' OR short_name = 'sbirt_in_the_past_three_months_has_your_use_of_hallucinogens_le')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_in_the_past_three_months_has_your_use_of_hallucinogens_le"
            END as "sbirt_in_the_past_three_months_has_your_use_of_hallucinogens_le",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_are_you_interested_in_learning_more_about_what_the_substa' OR linkid = 'sbirt_are_you_interested_in_learning_more_about_what_the_substa' OR short_name = 'sbirt_are_you_interested_in_learning_more_about_what_the_substa')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_are_you_interested_in_learning_more_about_what_the_substa"
            END as "sbirt_are_you_interested_in_learning_more_about_what_the_substa",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_during_the_past_three_months_how_often_have_you_had_a_s_8' OR linkid = 'sbirt_during_the_past_three_months_how_often_have_you_had_a_s_8' OR short_name = 'sbirt_during_the_past_three_months_how_often_have_you_had_a_s_8')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_during_the_past_three_months_how_often_have_you_had_a_s_8"
            END as "sbirt_during_the_past_three_months_how_often_have_you_had_a_s_8",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_age_7' OR linkid = 'add_family_member_registration_calculated_age_7' OR short_name = 'add_family_member_registration_calculated_age_7')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_age_7"
            END as "add_family_member_registration_calculated_age_7",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_do_you_ever_feel_bad_or_guilty_about_your_drug_use' OR linkid = 'sbirt_do_you_ever_feel_bad_or_guilty_about_your_drug_use' OR short_name = 'sbirt_do_you_ever_feel_bad_or_guilty_about_your_drug_use')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_do_you_ever_feel_bad_or_guilty_about_your_drug_use"
            END as "sbirt_do_you_ever_feel_bad_or_guilty_about_your_drug_use",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_things_others_can_help_you_do' OR linkid = 'sbirt_things_others_can_help_you_do' OR short_name = 'sbirt_things_others_can_help_you_do')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_things_others_can_help_you_do"
            END as "sbirt_things_others_can_help_you_do",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__9' OR linkid = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__9' OR short_name = 'sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__9')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__9"
            END as "sbirt_has_a_friend_or_relative_or_anyone_else_ever_expressed__9",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_5' OR linkid = 'sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_5' OR short_name = 'sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_5"
            END as "sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_in_the_past_three_months_how_often_have_you_used_sedative' OR linkid = 'sbirt_in_the_past_three_months_how_often_have_you_used_sedative' OR short_name = 'sbirt_in_the_past_three_months_how_often_have_you_used_sedative')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_in_the_past_three_months_how_often_have_you_used_sedative"
            END as "sbirt_in_the_past_three_months_how_often_have_you_used_sedative",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_have_you_had_medical_problems_as_a_result_of_your_drug_us' OR linkid = 'sbirt_have_you_had_medical_problems_as_a_result_of_your_drug_us' OR short_name = 'sbirt_have_you_had_medical_problems_as_a_result_of_your_drug_us')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_have_you_had_medical_problems_as_a_result_of_your_drug_us"
            END as "sbirt_have_you_had_medical_problems_as_a_result_of_your_drug_us",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_have_you_ever_used_any_drugs_by_injection_non_medical_use' OR linkid = 'sbirt_have_you_ever_used_any_drugs_by_injection_non_medical_use' OR short_name = 'sbirt_have_you_ever_used_any_drugs_by_injection_non_medical_use')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_have_you_ever_used_any_drugs_by_injection_non_medical_use"
            END as "sbirt_have_you_ever_used_any_drugs_by_injection_non_medical_use",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_other_drugs' OR linkid = 'sbirt_other_drugs' OR short_name = 'sbirt_other_drugs')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_other_drugs"
            END as "sbirt_other_drugs",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_have_you_engaged_in_illegal_activities_in_order_to_obtain' OR linkid = 'sbirt_have_you_engaged_in_illegal_activities_in_order_to_obtain' OR short_name = 'sbirt_have_you_engaged_in_illegal_activities_in_order_to_obtain')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_have_you_engaged_in_illegal_activities_in_order_to_obtain"
            END as "sbirt_have_you_engaged_in_illegal_activities_in_order_to_obtain",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_in_the_past_three_months_how_often_have_you_used_prescrip' OR linkid = 'sbirt_in_the_past_three_months_how_often_have_you_used_prescrip' OR short_name = 'sbirt_in_the_past_three_months_how_often_have_you_used_prescrip')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_in_the_past_three_months_how_often_have_you_used_prescrip"
            END as "sbirt_in_the_past_three_months_how_often_have_you_used_prescrip",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_month_7' OR linkid = 'add_family_member_registration_calculated_month_7' OR short_name = 'add_family_member_registration_calculated_month_7')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_month_7"
            END as "add_family_member_registration_calculated_month_7",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_year_7' OR linkid = 'add_family_member_registration_calculated_year_7' OR short_name = 'add_family_member_registration_calculated_year_7')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_year_7"
            END as "add_family_member_registration_calculated_year_7",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_in_the_past_three_months_how_often_have_you_used_methamph' OR linkid = 'sbirt_in_the_past_three_months_how_often_have_you_used_methamph' OR short_name = 'sbirt_in_the_past_three_months_how_often_have_you_used_methamph')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_in_the_past_three_months_how_often_have_you_used_methamph"
            END as "sbirt_in_the_past_three_months_how_often_have_you_used_methamph",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_date_of_birth_7' OR linkid = 'add_family_member_registration_date_of_birth_7' OR short_name = 'add_family_member_registration_date_of_birth_7')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_date_of_birth_7"
            END as "add_family_member_registration_date_of_birth_7",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_during_the_past_three_months_how_often_have_you_had_a_s_9' OR linkid = 'sbirt_during_the_past_three_months_how_often_have_you_had_a_s_9' OR short_name = 'sbirt_during_the_past_three_months_how_often_have_you_had_a_s_9')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_during_the_past_three_months_how_often_have_you_had_a_s_9"
            END as "sbirt_during_the_past_three_months_how_often_have_you_had_a_s_9",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_how_often_during_the_last_year_have_you_failed_to_do_what' OR linkid = 'sbirt_how_often_during_the_last_year_have_you_failed_to_do_what' OR short_name = 'sbirt_how_often_during_the_last_year_have_you_failed_to_do_what')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_how_often_during_the_last_year_have_you_failed_to_do_what"
            END as "sbirt_how_often_during_the_last_year_have_you_failed_to_do_what",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_in_the_past_three_months_has_your_use_of_cocaine_led_to_h' OR linkid = 'sbirt_in_the_past_three_months_has_your_use_of_cocaine_led_to_h' OR short_name = 'sbirt_in_the_past_three_months_has_your_use_of_cocaine_led_to_h')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_in_the_past_three_months_has_your_use_of_cocaine_led_to_h"
            END as "sbirt_in_the_past_three_months_has_your_use_of_cocaine_led_to_h",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_in_the_past_three_months_how_often_have_you_used_cannabis' OR linkid = 'sbirt_in_the_past_three_months_how_often_have_you_used_cannabis' OR short_name = 'sbirt_in_the_past_three_months_how_often_have_you_used_cannabis')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_in_the_past_three_months_how_often_have_you_used_cannabis"
            END as "sbirt_in_the_past_three_months_how_often_have_you_used_cannabis",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_7' OR linkid = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_7' OR short_name = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_7')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_7"
            END as "sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_7",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_6' OR linkid = 'sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_6' OR short_name = 'sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_6"
            END as "sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_does_your_spouse_or_parents_ever_complain_about_your_invo' OR linkid = 'sbirt_does_your_spouse_or_parents_ever_complain_about_your_invo' OR short_name = 'sbirt_does_your_spouse_or_parents_ever_complain_about_your_invo')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_does_your_spouse_or_parents_ever_complain_about_your_invo"
            END as "sbirt_does_your_spouse_or_parents_ever_complain_about_your_invo",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_in_the_past_three_months_how_often_have_you_used_street_o' OR linkid = 'sbirt_in_the_past_three_months_how_often_have_you_used_street_o' OR short_name = 'sbirt_in_the_past_three_months_how_often_have_you_used_street_o')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_in_the_past_three_months_how_often_have_you_used_street_o"
            END as "sbirt_in_the_past_three_months_how_often_have_you_used_street_o",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_on_a_scale_of_0_to_10_with_0_being_not_ready_at_all_and_1' OR linkid = 'sbirt_on_a_scale_of_0_to_10_with_0_being_not_ready_at_all_and_1' OR short_name = 'sbirt_on_a_scale_of_0_to_10_with_0_being_not_ready_at_all_and_1')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_on_a_scale_of_0_to_10_with_0_being_not_ready_at_all_and_1"
            END as "sbirt_on_a_scale_of_0_to_10_with_0_being_not_ready_at_all_and_1",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_in_the_past_three_months_how_often_have_you_used_inhalant' OR linkid = 'sbirt_in_the_past_three_months_how_often_have_you_used_inhalant' OR short_name = 'sbirt_in_the_past_three_months_how_often_have_you_used_inhalant')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_in_the_past_three_months_how_often_have_you_used_inhalant"
            END as "sbirt_in_the_past_three_months_how_often_have_you_used_inhalant",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_was_the_client_referred' OR linkid = 'sbirt_was_the_client_referred' OR short_name = 'sbirt_was_the_client_referred')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_was_the_client_referred"
            END as "sbirt_was_the_client_referred",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_drug_to_use_alcohol' OR linkid = 'sbirt_drug_to_use_alcohol' OR short_name = 'sbirt_drug_to_use_alcohol')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_drug_to_use_alcohol"
            END as "sbirt_drug_to_use_alcohol",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_drug_to_use_marijuana_or_other' OR linkid = 'sbirt_drug_to_use_marijuana_or_other' OR short_name = 'sbirt_drug_to_use_marijuana_or_other')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_drug_to_use_marijuana_or_other"
            END as "sbirt_drug_to_use_marijuana_or_other",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_in_the_past_three_months_has_your_use_of_sedatives_led_to' OR linkid = 'sbirt_in_the_past_three_months_has_your_use_of_sedatives_led_to' OR short_name = 'sbirt_in_the_past_three_months_has_your_use_of_sedatives_led_to')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_in_the_past_three_months_has_your_use_of_sedatives_led_to"
            END as "sbirt_in_the_past_three_months_has_your_use_of_sedatives_led_to",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_7' OR linkid = 'sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_7' OR short_name = 'sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_7')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_7"
            END as "sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_7",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_8' OR linkid = 'sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_8' OR short_name = 'sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_8')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_8"
            END as "sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_8",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_in_the_past_three_months_how_often_have_you_used_hallucin' OR linkid = 'sbirt_in_the_past_three_months_how_often_have_you_used_hallucin' OR short_name = 'sbirt_in_the_past_three_months_how_often_have_you_used_hallucin')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_in_the_past_three_months_how_often_have_you_used_hallucin"
            END as "sbirt_in_the_past_three_months_how_often_have_you_used_hallucin",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_would_you_be_interested_in_learning_more_about_substance_' OR linkid = 'sbirt_would_you_be_interested_in_learning_more_about_substance_' OR short_name = 'sbirt_would_you_be_interested_in_learning_more_about_substance_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_would_you_be_interested_in_learning_more_about_substance_"
            END as "sbirt_would_you_be_interested_in_learning_more_about_substance_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_are_you_always_able_to_stop_using_drugs_when_you_want_to' OR linkid = 'sbirt_are_you_always_able_to_stop_using_drugs_when_you_want_to' OR short_name = 'sbirt_are_you_always_able_to_stop_using_drugs_when_you_want_to')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_are_you_always_able_to_stop_using_drugs_when_you_want_to"
            END as "sbirt_are_you_always_able_to_stop_using_drugs_when_you_want_to",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_things_you_can_do' OR linkid = 'sbirt_things_you_can_do' OR short_name = 'sbirt_things_you_can_do')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_things_you_can_do"
            END as "sbirt_things_you_can_do",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_on_average_how_many_days_per_week_do_you_use_prescription' OR linkid = 'sbirt_on_average_how_many_days_per_week_do_you_use_prescription' OR short_name = 'sbirt_on_average_how_many_days_per_week_do_you_use_prescription')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_on_average_how_many_days_per_week_do_you_use_prescription"
            END as "sbirt_on_average_how_many_days_per_week_do_you_use_prescription",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_sedatives_just_for_the_feeling_more_than_prescribed_or_th' OR linkid = 'sbirt_sedatives_just_for_the_feeling_more_than_prescribed_or_th' OR short_name = 'sbirt_sedatives_just_for_the_feeling_more_than_prescribed_or_th')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_sedatives_just_for_the_feeling_more_than_prescribed_or_th"
            END as "sbirt_sedatives_just_for_the_feeling_more_than_prescribed_or_th",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_what_are_some_good_things_about_making_a_change' OR linkid = 'sbirt_what_are_some_good_things_about_making_a_change' OR short_name = 'sbirt_what_are_some_good_things_about_making_a_change')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_what_are_some_good_things_about_making_a_change"
            END as "sbirt_what_are_some_good_things_about_making_a_change",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_how_often_during_the_last_year_have_you_found_that_you_we' OR linkid = 'sbirt_how_often_during_the_last_year_have_you_found_that_you_we' OR short_name = 'sbirt_how_often_during_the_last_year_have_you_found_that_you_we')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_how_often_during_the_last_year_have_you_found_that_you_we"
            END as "sbirt_how_often_during_the_last_year_have_you_found_that_you_we",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'survey_response_7' OR linkid = 'survey_response_7' OR short_name = 'survey_response_7')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "survey_response_7"
            END as "survey_response_7",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_have_you_used_drugs_other_than_those_required_for_medical' OR linkid = 'sbirt_have_you_used_drugs_other_than_those_required_for_medical' OR short_name = 'sbirt_have_you_used_drugs_other_than_those_required_for_medical')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_have_you_used_drugs_other_than_those_required_for_medical"
            END as "sbirt_have_you_used_drugs_other_than_those_required_for_medical",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_has_a_relative_or_friend_or_a_doctor_or_another_health_wo' OR linkid = 'sbirt_has_a_relative_or_friend_or_a_doctor_or_another_health_wo' OR short_name = 'sbirt_has_a_relative_or_friend_or_a_doctor_or_another_health_wo')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_has_a_relative_or_friend_or_a_doctor_or_another_health_wo"
            END as "sbirt_has_a_relative_or_friend_or_a_doctor_or_another_health_wo",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_9' OR linkid = 'sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_9' OR short_name = 'sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_9')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_9"
            END as "sbirt_in_the_past_three_months_how_often_have_you_failed_to_d_9",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_methamphetamine_meth_crystal_speed_ecstasy_molly_etc' OR linkid = 'sbirt_methamphetamine_meth_crystal_speed_ecstasy_molly_etc' OR short_name = 'sbirt_methamphetamine_meth_crystal_speed_ecstasy_molly_etc')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_methamphetamine_meth_crystal_speed_ecstasy_molly_etc"
            END as "sbirt_methamphetamine_meth_crystal_speed_ecstasy_molly_etc",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_8' OR linkid = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_8' OR short_name = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_8')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_8"
            END as "sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_8",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_9' OR linkid = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_9' OR short_name = 'sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_9')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_9"
            END as "sbirt_have_you_ever_tried_and_failed_to_control_cut_down_or_s_9",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'survey_response_8' OR linkid = 'survey_response_8' OR short_name = 'survey_response_8')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "survey_response_8"
            END as "survey_response_8",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_do_you_use_more_than_one_drug_at_a_time' OR linkid = 'sbirt_do_you_use_more_than_one_drug_at_a_time' OR short_name = 'sbirt_do_you_use_more_than_one_drug_at_a_time')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_do_you_use_more_than_one_drug_at_a_time"
            END as "sbirt_do_you_use_more_than_one_drug_at_a_time",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_can_i_share_additional_treatment_options_with_you' OR linkid = 'sbirt_can_i_share_additional_treatment_options_with_you' OR short_name = 'sbirt_can_i_share_additional_treatment_options_with_you')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_can_i_share_additional_treatment_options_with_you"
            END as "sbirt_can_i_share_additional_treatment_options_with_you",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_health_goals_do_not_want_to_change' OR linkid = 'sbirt_health_goals_do_not_want_to_change' OR short_name = 'sbirt_health_goals_do_not_want_to_change')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_health_goals_do_not_want_to_change"
            END as "sbirt_health_goals_do_not_want_to_change",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_is_drug_dependence' OR linkid = 'sbirt_is_drug_dependence' OR short_name = 'sbirt_is_drug_dependence')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_is_drug_dependence"
            END as "sbirt_is_drug_dependence",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_is_high_risk_alcohol' OR linkid = 'sbirt_is_high_risk_alcohol' OR short_name = 'sbirt_is_high_risk_alcohol')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_is_high_risk_alcohol"
            END as "sbirt_is_high_risk_alcohol",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_is_high_risk_drug' OR linkid = 'sbirt_is_high_risk_drug' OR short_name = 'sbirt_is_high_risk_drug')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_is_high_risk_drug"
            END as "sbirt_is_high_risk_drug",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_is_low_risk_alcohol' OR linkid = 'sbirt_is_low_risk_alcohol' OR short_name = 'sbirt_is_low_risk_alcohol')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_is_low_risk_alcohol"
            END as "sbirt_is_low_risk_alcohol",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_is_low_risk_drug' OR linkid = 'sbirt_is_low_risk_drug' OR short_name = 'sbirt_is_low_risk_drug')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_is_low_risk_drug"
            END as "sbirt_is_low_risk_drug",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_is_medium_risk_alcohol' OR linkid = 'sbirt_is_medium_risk_alcohol' OR short_name = 'sbirt_is_medium_risk_alcohol')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_is_medium_risk_alcohol"
            END as "sbirt_is_medium_risk_alcohol",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_is_medium_risk_drug' OR linkid = 'sbirt_is_medium_risk_drug' OR short_name = 'sbirt_is_medium_risk_drug')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_is_medium_risk_drug"
            END as "sbirt_is_medium_risk_drug",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_is_opioid_dependence' OR linkid = 'sbirt_is_opioid_dependence' OR short_name = 'sbirt_is_opioid_dependence')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_is_opioid_dependence"
            END as "sbirt_is_opioid_dependence",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_other_drug_list' OR linkid = 'sbirt_other_drug_list' OR short_name = 'sbirt_other_drug_list')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_other_drug_list"
            END as "sbirt_other_drug_list",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'patient-age' OR linkid = 'patient-age' OR short_name = 'patient-age')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "patient-age"
            END as "patient-age",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'patient-dob' OR linkid = 'patient-dob' OR short_name = 'patient-dob')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "patient-dob"
            END as "patient-dob",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'patient-gender' OR linkid = 'patient-gender' OR short_name = 'patient-gender')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "patient-gender"
            END as "patient-gender",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'patient-name' OR linkid = 'patient-name' OR short_name = 'patient-name')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "patient-name"
            END as "patient-name",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_score_audit' OR linkid = 'sbirt_score_audit' OR short_name = 'sbirt_score_audit')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_score_audit"
            END as "sbirt_score_audit",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_score_dast' OR linkid = 'sbirt_score_dast' OR short_name = 'sbirt_score_dast')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_score_dast"
            END as "sbirt_score_dast",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_should_converse_about_substance_use' OR linkid = 'sbirt_should_converse_about_substance_use' OR short_name = 'sbirt_should_converse_about_substance_use')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_should_converse_about_substance_use"
            END as "sbirt_should_converse_about_substance_use",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_should_discuss_health_goals' OR linkid = 'sbirt_should_discuss_health_goals' OR short_name = 'sbirt_should_discuss_health_goals')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_should_discuss_health_goals"
            END as "sbirt_should_discuss_health_goals",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_show_client_use_with_other_drugs' OR linkid = 'sbirt_show_client_use_with_other_drugs' OR short_name = 'sbirt_show_client_use_with_other_drugs')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_show_client_use_with_other_drugs"
            END as "sbirt_show_client_use_with_other_drugs",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_show_discuss_next_steps' OR linkid = 'sbirt_show_discuss_next_steps' OR short_name = 'sbirt_show_discuss_next_steps')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_show_discuss_next_steps"
            END as "sbirt_show_discuss_next_steps",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_show_goals' OR linkid = 'sbirt_show_goals' OR short_name = 'sbirt_show_goals')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_show_goals"
            END as "sbirt_show_goals",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_show_intervention_when_low_risk' OR linkid = 'sbirt_show_intervention_when_low_risk' OR short_name = 'sbirt_show_intervention_when_low_risk')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_show_intervention_when_low_risk"
            END as "sbirt_show_intervention_when_low_risk",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_show_intervention_when_medium_high_risk' OR linkid = 'sbirt_show_intervention_when_medium_high_risk' OR short_name = 'sbirt_show_intervention_when_medium_high_risk')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_show_intervention_when_medium_high_risk"
            END as "sbirt_show_intervention_when_medium_high_risk",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_show_is_medium_high_risk' OR linkid = 'sbirt_show_is_medium_high_risk' OR short_name = 'sbirt_show_is_medium_high_risk')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_show_is_medium_high_risk"
            END as "sbirt_show_is_medium_high_risk",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_show_summary_for_low_risk' OR linkid = 'sbirt_show_summary_for_low_risk' OR short_name = 'sbirt_show_summary_for_low_risk')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_show_summary_for_low_risk"
            END as "sbirt_show_summary_for_low_risk",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_show_things_to_do' OR linkid = 'sbirt_show_things_to_do' OR short_name = 'sbirt_show_things_to_do')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_show_things_to_do"
            END as "sbirt_show_things_to_do",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_task_id_sbirt_pdf' OR linkid = 'sbirt_task_id_sbirt_pdf' OR short_name = 'sbirt_task_id_sbirt_pdf')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_task_id_sbirt_pdf"
            END as "sbirt_task_id_sbirt_pdf",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'sbirt_using_opioids_and_low_risk' OR linkid = 'sbirt_using_opioids_and_low_risk' OR short_name = 'sbirt_using_opioids_and_low_risk')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "sbirt_using_opioids_and_low_risk"
            END as "sbirt_using_opioids_and_low_risk",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data


  );