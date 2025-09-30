

-- Anonymized view for qr_financial_wellness_tool
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_financial_wellness_tool'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_financial_wellness_tool"
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
                    WHERE ("column" = 'financial_wellness_tool_in_the_past_12_months_was_there_a_time_' OR linkid = 'financial_wellness_tool_in_the_past_12_months_was_there_a_time_' OR short_name = 'financial_wellness_tool_in_the_past_12_months_was_there_a_time_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "financial_wellness_tool_in_the_past_12_months_was_there_a_time_"
            END as "financial_wellness_tool_in_the_past_12_months_was_there_a_time_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'financial_wellness_tool_in_the_past_12_months_did_you_not_pay_t' OR linkid = 'financial_wellness_tool_in_the_past_12_months_did_you_not_pay_t' OR short_name = 'financial_wellness_tool_in_the_past_12_months_did_you_not_pay_t')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "financial_wellness_tool_in_the_past_12_months_did_you_not_pay_t"
            END as "financial_wellness_tool_in_the_past_12_months_did_you_not_pay_t",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'financial_wellness_tool_how_hard_is_it_for_you_to_pay_for_the_v' OR linkid = 'financial_wellness_tool_how_hard_is_it_for_you_to_pay_for_the_v' OR short_name = 'financial_wellness_tool_how_hard_is_it_for_you_to_pay_for_the_v')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "financial_wellness_tool_how_hard_is_it_for_you_to_pay_for_the_v"
            END as "financial_wellness_tool_how_hard_is_it_for_you_to_pay_for_the_v",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'financial_wellness_tool_how_strongly_do_you_agree_or_disagree_w' OR linkid = 'financial_wellness_tool_how_strongly_do_you_agree_or_disagree_w' OR short_name = 'financial_wellness_tool_how_strongly_do_you_agree_or_disagree_w')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "financial_wellness_tool_how_strongly_do_you_agree_or_disagree_w"
            END as "financial_wellness_tool_how_strongly_do_you_agree_or_disagree_w",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'financial_wellness_tool_in_the_past_12_months_did_you_move_in_w' OR linkid = 'financial_wellness_tool_in_the_past_12_months_did_you_move_in_w' OR short_name = 'financial_wellness_tool_in_the_past_12_months_did_you_move_in_w')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "financial_wellness_tool_in_the_past_12_months_did_you_move_in_w"
            END as "financial_wellness_tool_in_the_past_12_months_did_you_move_in_w",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'financial_wellness_tool_in_the_past_12_months_how_often_did_you' OR linkid = 'financial_wellness_tool_in_the_past_12_months_how_often_did_you' OR short_name = 'financial_wellness_tool_in_the_past_12_months_how_often_did_you')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "financial_wellness_tool_in_the_past_12_months_how_often_did_you"
            END as "financial_wellness_tool_in_the_past_12_months_how_often_did_you",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'financial_wellness_tool_in_the_past_12_months_did_you_stay_at_a' OR linkid = 'financial_wellness_tool_in_the_past_12_months_did_you_stay_at_a' OR short_name = 'financial_wellness_tool_in_the_past_12_months_did_you_stay_at_a')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "financial_wellness_tool_in_the_past_12_months_did_you_stay_at_a"
            END as "financial_wellness_tool_in_the_past_12_months_did_you_stay_at_a",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'financial_wellness_tool_in_the_past_12_months_did_you_not_pay_2' OR linkid = 'financial_wellness_tool_in_the_past_12_months_did_you_not_pay_2' OR short_name = 'financial_wellness_tool_in_the_past_12_months_did_you_not_pay_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "financial_wellness_tool_in_the_past_12_months_did_you_not_pay_2"
            END as "financial_wellness_tool_in_the_past_12_months_did_you_not_pay_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'financial_wellness_tool_i_worried_whether_my_food_would_run_out' OR linkid = 'financial_wellness_tool_i_worried_whether_my_food_would_run_out' OR short_name = 'financial_wellness_tool_i_worried_whether_my_food_would_run_out')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "financial_wellness_tool_i_worried_whether_my_food_would_run_out"
            END as "financial_wellness_tool_i_worried_whether_my_food_would_run_out",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'financial_wellness_tool_the_food_that_i_bought_just_didn_t_last' OR linkid = 'financial_wellness_tool_the_food_that_i_bought_just_didn_t_last' OR short_name = 'financial_wellness_tool_the_food_that_i_bought_just_didn_t_last')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "financial_wellness_tool_the_food_that_i_bought_just_didn_t_last"
            END as "financial_wellness_tool_the_food_that_i_bought_just_didn_t_last",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'financial_wellness_tool_in_the_past_12_months_was_your_phone_ga' OR linkid = 'financial_wellness_tool_in_the_past_12_months_was_your_phone_ga' OR short_name = 'financial_wellness_tool_in_the_past_12_months_was_your_phone_ga')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        CASE
                            WHEN "financial_wellness_tool_in_the_past_12_months_was_your_phone_ga" IS NOT NULL AND LENGTH("financial_wellness_tool_in_the_past_12_months_was_your_phone_ga"::text) >= 4
                            THEN 'XXX-XXX-' || RIGHT("financial_wellness_tool_in_the_past_12_months_was_your_phone_ga"::text, 4)
                            ELSE 'NO PHONE'
                        END
                    
                ELSE "financial_wellness_tool_in_the_past_12_months_was_your_phone_ga"
            END as "financial_wellness_tool_in_the_past_12_months_was_your_phone_ga",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'financial_wellness_tool_when_you_think_about_your_financial_sit' OR linkid = 'financial_wellness_tool_when_you_think_about_your_financial_sit' OR short_name = 'financial_wellness_tool_when_you_think_about_your_financial_sit')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "financial_wellness_tool_when_you_think_about_your_financial_sit"
            END as "financial_wellness_tool_when_you_think_about_your_financial_sit",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'financial_wellness_tool' OR linkid = 'financial_wellness_tool' OR short_name = 'financial_wellness_tool')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "financial_wellness_tool"
            END as "financial_wellness_tool",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'financial_wellness_tool_2' OR linkid = 'financial_wellness_tool_2' OR short_name = 'financial_wellness_tool_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "financial_wellness_tool_2"
            END as "financial_wellness_tool_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'financial_wellness_tool_3' OR linkid = 'financial_wellness_tool_3' OR short_name = 'financial_wellness_tool_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "financial_wellness_tool_3"
            END as "financial_wellness_tool_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'financial_wellness_tool_4' OR linkid = 'financial_wellness_tool_4' OR short_name = 'financial_wellness_tool_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "financial_wellness_tool_4"
            END as "financial_wellness_tool_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'financial_wellness_tool_5' OR linkid = 'financial_wellness_tool_5' OR short_name = 'financial_wellness_tool_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "financial_wellness_tool_5"
            END as "financial_wellness_tool_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'financial_wellness_tool_6' OR linkid = 'financial_wellness_tool_6' OR short_name = 'financial_wellness_tool_6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "financial_wellness_tool_6"
            END as "financial_wellness_tool_6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'financial_wellness_tool_7' OR linkid = 'financial_wellness_tool_7' OR short_name = 'financial_wellness_tool_7')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "financial_wellness_tool_7"
            END as "financial_wellness_tool_7",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'financial_wellness_tool_8' OR linkid = 'financial_wellness_tool_8' OR short_name = 'financial_wellness_tool_8')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "financial_wellness_tool_8"
            END as "financial_wellness_tool_8",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'financial_wellness_tool_9' OR linkid = 'financial_wellness_tool_9' OR short_name = 'financial_wellness_tool_9')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "financial_wellness_tool_9"
            END as "financial_wellness_tool_9",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'financial_wellness_tool_10' OR linkid = 'financial_wellness_tool_10' OR short_name = 'financial_wellness_tool_10')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "financial_wellness_tool_10"
            END as "financial_wellness_tool_10",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'financial_wellness_tool_11' OR linkid = 'financial_wellness_tool_11' OR short_name = 'financial_wellness_tool_11')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "financial_wellness_tool_11"
            END as "financial_wellness_tool_11",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data

