

-- Anonymized view for qr_spi_subform_4
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_spi_subform_4'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_spi_subform_4"
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
                    WHERE ("column" = 'spi_item' OR linkid = 'spi_item' OR short_name = 'spi_item')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_item"
            END as "spi_item",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4' OR linkid = 'spi_subform_4' OR short_name = 'spi_subform_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4"
            END as "spi_subform_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_serv' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_serv' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_serv')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_serv"
            END as "review_the_safety_planstep_5_professionals_or_professional_serv",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_se_2' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_se_2' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_se_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_se_2"
            END as "review_the_safety_planstep_5_professionals_or_professional_se_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_se_3' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_se_3' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_se_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_se_3"
            END as "review_the_safety_planstep_5_professionals_or_professional_se_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_taking_pills_i_will_protect_myself_from_this_risk' OR linkid = 'spi_subform_4_taking_pills_i_will_protect_myself_from_this_risk' OR short_name = 'spi_subform_4_taking_pills_i_will_protect_myself_from_this_risk')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_taking_pills_i_will_protect_myself_from_this_risk"
            END as "spi_subform_4_taking_pills_i_will_protect_myself_from_this_risk",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_duri' OR linkid = 'step_5professionals_or_professional_services_that_can_help_duri' OR short_name = 'step_5professionals_or_professional_services_that_can_help_duri')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_duri"
            END as "step_5professionals_or_professional_services_that_can_help_duri",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_2' OR linkid = 'spi_subform_4_2' OR short_name = 'spi_subform_4_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_2"
            END as "spi_subform_4_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_4_family_members_or_friends_who_may_' OR linkid = 'review_the_safety_planstep_4_family_members_or_friends_who_may_' OR short_name = 'review_the_safety_planstep_4_family_members_or_friends_who_may_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_4_family_members_or_friends_who_may_"
            END as "review_the_safety_planstep_4_family_members_or_friends_who_may_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_3' OR linkid = 'spi_subform_4_3' OR short_name = 'spi_subform_4_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_3"
            END as "spi_subform_4_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_du_2' OR linkid = 'step_5professionals_or_professional_services_that_can_help_du_2' OR short_name = 'step_5professionals_or_professional_services_that_can_help_du_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_du_2"
            END as "step_5professionals_or_professional_services_that_can_help_du_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_du_3' OR linkid = 'step_5professionals_or_professional_services_that_can_help_du_3' OR short_name = 'step_5professionals_or_professional_services_that_can_help_du_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_du_3"
            END as "step_5professionals_or_professional_services_that_can_help_du_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_2_internal_coping_strategies' OR linkid = 'review_the_safety_planstep_2_internal_coping_strategies' OR short_name = 'review_the_safety_planstep_2_internal_coping_strategies')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_2_internal_coping_strategies"
            END as "review_the_safety_planstep_2_internal_coping_strategies",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_4' OR linkid = 'spi_subform_4_4' OR short_name = 'spi_subform_4_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_4"
            END as "spi_subform_4_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_item_2' OR linkid = 'spi_item_2' OR short_name = 'spi_item_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_item_2"
            END as "spi_item_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_se_4' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_se_4' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_se_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_se_4"
            END as "review_the_safety_planstep_5_professionals_or_professional_se_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_5' OR linkid = 'spi_subform_4_5' OR short_name = 'spi_subform_4_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_5"
            END as "spi_subform_4_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_se_5' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_se_5' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_se_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_se_5"
            END as "review_the_safety_planstep_5_professionals_or_professional_se_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_du_4' OR linkid = 'step_5professionals_or_professional_services_that_can_help_du_4' OR short_name = 'step_5professionals_or_professional_services_that_can_help_du_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_du_4"
            END as "step_5professionals_or_professional_services_that_can_help_du_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_du_5' OR linkid = 'step_5professionals_or_professional_services_that_can_help_du_5' OR short_name = 'step_5professionals_or_professional_services_that_can_help_du_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_du_5"
            END as "step_5professionals_or_professional_services_that_can_help_du_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_6_making_the_environment_safe_cuttin' OR linkid = 'review_the_safety_planstep_6_making_the_environment_safe_cuttin' OR short_name = 'review_the_safety_planstep_6_making_the_environment_safe_cuttin')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_6_making_the_environment_safe_cuttin"
            END as "review_the_safety_planstep_6_making_the_environment_safe_cuttin",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_jumping_i_will_protect_myself_from_this_risk_by' OR linkid = 'spi_subform_4_jumping_i_will_protect_myself_from_this_risk_by' OR short_name = 'spi_subform_4_jumping_i_will_protect_myself_from_this_risk_by')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_jumping_i_will_protect_myself_from_this_risk_by"
            END as "spi_subform_4_jumping_i_will_protect_myself_from_this_risk_by",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_hanging_i_will_protect_myself_from_this_risk_by' OR linkid = 'spi_subform_4_hanging_i_will_protect_myself_from_this_risk_by' OR short_name = 'spi_subform_4_hanging_i_will_protect_myself_from_this_risk_by')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_hanging_i_will_protect_myself_from_this_risk_by"
            END as "spi_subform_4_hanging_i_will_protect_myself_from_this_risk_by",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_in_a_few_days_i_would_like_to_meet_for_a_follow_u' OR linkid = 'spi_subform_4_in_a_few_days_i_would_like_to_meet_for_a_follow_u' OR short_name = 'spi_subform_4_in_a_few_days_i_would_like_to_meet_for_a_follow_u')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_in_a_few_days_i_would_like_to_meet_for_a_follow_u"
            END as "spi_subform_4_in_a_few_days_i_would_like_to_meet_for_a_follow_u",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_6' OR linkid = 'spi_subform_4_6' OR short_name = 'spi_subform_4_6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_6"
            END as "spi_subform_4_6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_6_making_the_environment_safe_jumpin' OR linkid = 'review_the_safety_planstep_6_making_the_environment_safe_jumpin' OR short_name = 'review_the_safety_planstep_6_making_the_environment_safe_jumpin')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_6_making_the_environment_safe_jumpin"
            END as "review_the_safety_planstep_6_making_the_environment_safe_jumpin",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_4family_members_or_friends_who_may_offer_help_are_you_will' OR linkid = 'step_4family_members_or_friends_who_may_offer_help_are_you_will' OR short_name = 'step_4family_members_or_friends_who_may_offer_help_are_you_will')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_4family_members_or_friends_who_may_offer_help_are_you_will"
            END as "step_4family_members_or_friends_who_may_offer_help_are_you_will",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_se_6' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_se_6' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_se_6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_se_6"
            END as "review_the_safety_planstep_5_professionals_or_professional_se_6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_7' OR linkid = 'spi_subform_4_7' OR short_name = 'spi_subform_4_7')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_7"
            END as "spi_subform_4_7",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_8' OR linkid = 'spi_subform_4_8' OR short_name = 'spi_subform_4_8')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_8"
            END as "spi_subform_4_8",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_9' OR linkid = 'spi_subform_4_9' OR short_name = 'spi_subform_4_9')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_9"
            END as "spi_subform_4_9",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_10' OR linkid = 'spi_subform_4_10' OR short_name = 'spi_subform_4_10')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_10"
            END as "spi_subform_4_10",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_se_7' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_se_7' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_se_7')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_se_7"
            END as "review_the_safety_planstep_5_professionals_or_professional_se_7",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_11' OR linkid = 'spi_subform_4_11' OR short_name = 'spi_subform_4_11')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_11"
            END as "spi_subform_4_11",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_se_8' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_se_8' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_se_8')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_se_8"
            END as "review_the_safety_planstep_5_professionals_or_professional_se_8",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_se_9' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_se_9' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_se_9')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_se_9"
            END as "review_the_safety_planstep_5_professionals_or_professional_se_9",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_12' OR linkid = 'spi_subform_4_12' OR short_name = 'spi_subform_4_12')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_12"
            END as "spi_subform_4_12",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_4_family_members_or_friends_who_ma_2' OR linkid = 'review_the_safety_planstep_4_family_members_or_friends_who_ma_2' OR short_name = 'review_the_safety_planstep_4_family_members_or_friends_who_ma_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_4_family_members_or_friends_who_ma_2"
            END as "review_the_safety_planstep_4_family_members_or_friends_who_ma_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_13' OR linkid = 'spi_subform_4_13' OR short_name = 'spi_subform_4_13')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_13"
            END as "spi_subform_4_13",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_s_10' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_s_10' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_s_10')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_s_10"
            END as "review_the_safety_planstep_5_professionals_or_professional_s_10",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_14' OR linkid = 'spi_subform_4_14' OR short_name = 'spi_subform_4_14')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_14"
            END as "spi_subform_4_14",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_item_3' OR linkid = 'spi_item_3' OR short_name = 'spi_item_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_item_3"
            END as "spi_item_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_item_4' OR linkid = 'spi_item_4' OR short_name = 'spi_item_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_item_4"
            END as "spi_item_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_15' OR linkid = 'spi_subform_4_15' OR short_name = 'spi_subform_4_15')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_15"
            END as "spi_subform_4_15",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_16' OR linkid = 'spi_subform_4_16' OR short_name = 'spi_subform_4_16')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_16"
            END as "spi_subform_4_16",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_s_11' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_s_11' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_s_11')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_s_11"
            END as "review_the_safety_planstep_5_professionals_or_professional_s_11",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_s_12' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_s_12' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_s_12')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_s_12"
            END as "review_the_safety_planstep_5_professionals_or_professional_s_12",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_s_13' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_s_13' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_s_13')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_s_13"
            END as "review_the_safety_planstep_5_professionals_or_professional_s_13",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_1_warning_signs' OR linkid = 'review_the_safety_planstep_1_warning_signs' OR short_name = 'review_the_safety_planstep_1_warning_signs')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_1_warning_signs"
            END as "review_the_safety_planstep_1_warning_signs",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_du_6' OR linkid = 'step_5professionals_or_professional_services_that_can_help_du_6' OR short_name = 'step_5professionals_or_professional_services_that_can_help_du_6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_du_6"
            END as "step_5professionals_or_professional_services_that_can_help_du_6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_s_14' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_s_14' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_s_14')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_s_14"
            END as "review_the_safety_planstep_5_professionals_or_professional_s_14",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_du_7' OR linkid = 'step_5professionals_or_professional_services_that_can_help_du_7' OR short_name = 'step_5professionals_or_professional_services_that_can_help_du_7')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_du_7"
            END as "step_5professionals_or_professional_services_that_can_help_du_7",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_17' OR linkid = 'spi_subform_4_17' OR short_name = 'spi_subform_4_17')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_17"
            END as "spi_subform_4_17",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_s_15' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_s_15' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_s_15')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_s_15"
            END as "review_the_safety_planstep_5_professionals_or_professional_s_15",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_item_5' OR linkid = 'spi_item_5' OR short_name = 'spi_item_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_item_5"
            END as "spi_item_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_18' OR linkid = 'spi_subform_4_18' OR short_name = 'spi_subform_4_18')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_18"
            END as "spi_subform_4_18",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_person_3' OR linkid = 'spi_subform_4_person_3' OR short_name = 'spi_subform_4_person_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_person_3"
            END as "spi_subform_4_person_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_du_8' OR linkid = 'step_5professionals_or_professional_services_that_can_help_du_8' OR short_name = 'step_5professionals_or_professional_services_that_can_help_du_8')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_du_8"
            END as "step_5professionals_or_professional_services_that_can_help_du_8",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_4family_members_or_friends_who_may_offer_help_are_you_wi_2' OR linkid = 'step_4family_members_or_friends_who_may_offer_help_are_you_wi_2' OR short_name = 'step_4family_members_or_friends_who_may_offer_help_are_you_wi_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_4family_members_or_friends_who_may_offer_help_are_you_wi_2"
            END as "step_4family_members_or_friends_who_may_offer_help_are_you_wi_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_the_methods_i_have_thought_about_using_include' OR linkid = 'spi_subform_4_the_methods_i_have_thought_about_using_include' OR short_name = 'spi_subform_4_the_methods_i_have_thought_about_using_include')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_the_methods_i_have_thought_about_using_include"
            END as "spi_subform_4_the_methods_i_have_thought_about_using_include",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_1_warning_signs_2' OR linkid = 'review_the_safety_planstep_1_warning_signs_2' OR short_name = 'review_the_safety_planstep_1_warning_signs_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_1_warning_signs_2"
            END as "review_the_safety_planstep_1_warning_signs_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_drowning_i_will_protect_myself_from_this_risk_by' OR linkid = 'spi_subform_4_drowning_i_will_protect_myself_from_this_risk_by' OR short_name = 'spi_subform_4_drowning_i_will_protect_myself_from_this_risk_by')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_drowning_i_will_protect_myself_from_this_risk_by"
            END as "spi_subform_4_drowning_i_will_protect_myself_from_this_risk_by",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_19' OR linkid = 'spi_subform_4_19' OR short_name = 'spi_subform_4_19')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_19"
            END as "spi_subform_4_19",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_20' OR linkid = 'spi_subform_4_20' OR short_name = 'spi_subform_4_20')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_20"
            END as "spi_subform_4_20",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_4family_members_or_friends_who_may_offer_help_person_2' OR linkid = 'step_4family_members_or_friends_who_may_offer_help_person_2' OR short_name = 'step_4family_members_or_friends_who_may_offer_help_person_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_4family_members_or_friends_who_may_offer_help_person_2"
            END as "step_4family_members_or_friends_who_may_offer_help_person_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_s_16' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_s_16' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_s_16')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_s_16"
            END as "review_the_safety_planstep_5_professionals_or_professional_s_16",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_21' OR linkid = 'spi_subform_4_21' OR short_name = 'spi_subform_4_21')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_21"
            END as "spi_subform_4_21",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_person_2' OR linkid = 'spi_subform_4_person_2' OR short_name = 'spi_subform_4_person_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_person_2"
            END as "spi_subform_4_person_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_item_6' OR linkid = 'spi_item_6' OR short_name = 'spi_item_6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_item_6"
            END as "spi_item_6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_s_17' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_s_17' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_s_17')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_s_17"
            END as "review_the_safety_planstep_5_professionals_or_professional_s_17",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_22' OR linkid = 'spi_subform_4_22' OR short_name = 'spi_subform_4_22')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_22"
            END as "spi_subform_4_22",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_23' OR linkid = 'spi_subform_4_23' OR short_name = 'spi_subform_4_23')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_23"
            END as "spi_subform_4_23",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_24' OR linkid = 'spi_subform_4_24' OR short_name = 'spi_subform_4_24')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_24"
            END as "spi_subform_4_24",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_25' OR linkid = 'spi_subform_4_25' OR short_name = 'spi_subform_4_25')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_25"
            END as "spi_subform_4_25",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_item_7' OR linkid = 'spi_item_7' OR short_name = 'spi_item_7')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_item_7"
            END as "spi_item_7",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_s_18' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_s_18' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_s_18')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_s_18"
            END as "review_the_safety_planstep_5_professionals_or_professional_s_18",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_26' OR linkid = 'spi_subform_4_26' OR short_name = 'spi_subform_4_26')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_26"
            END as "spi_subform_4_26",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_item_8' OR linkid = 'spi_item_8' OR short_name = 'spi_item_8')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_item_8"
            END as "spi_item_8",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_4family_members_or_friends_who_may_offer_help' OR linkid = 'step_4family_members_or_friends_who_may_offer_help' OR short_name = 'step_4family_members_or_friends_who_may_offer_help')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_4family_members_or_friends_who_may_offer_help"
            END as "step_4family_members_or_friends_who_may_offer_help",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_person_4' OR linkid = 'spi_subform_4_person_4' OR short_name = 'spi_subform_4_person_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_person_4"
            END as "spi_subform_4_person_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_s_19' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_s_19' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_s_19')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_s_19"
            END as "review_the_safety_planstep_5_professionals_or_professional_s_19",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_s_20' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_s_20' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_s_20')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_s_20"
            END as "review_the_safety_planstep_5_professionals_or_professional_s_20",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_s_21' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_s_21' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_s_21')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_s_21"
            END as "review_the_safety_planstep_5_professionals_or_professional_s_21",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_cutting_i_will_protect_myself_from_this_risk_by' OR linkid = 'spi_subform_4_cutting_i_will_protect_myself_from_this_risk_by' OR short_name = 'spi_subform_4_cutting_i_will_protect_myself_from_this_risk_by')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_cutting_i_will_protect_myself_from_this_risk_by"
            END as "spi_subform_4_cutting_i_will_protect_myself_from_this_risk_by",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_1_warning_signs_3' OR linkid = 'review_the_safety_planstep_1_warning_signs_3' OR short_name = 'review_the_safety_planstep_1_warning_signs_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_1_warning_signs_3"
            END as "review_the_safety_planstep_1_warning_signs_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_s_22' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_s_22' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_s_22')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_s_22"
            END as "review_the_safety_planstep_5_professionals_or_professional_s_22",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_4_family_members_or_friends_who_ma_3' OR linkid = 'review_the_safety_planstep_4_family_members_or_friends_who_ma_3' OR short_name = 'review_the_safety_planstep_4_family_members_or_friends_who_ma_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_4_family_members_or_friends_who_ma_3"
            END as "review_the_safety_planstep_4_family_members_or_friends_who_ma_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_s_23' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_s_23' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_s_23')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_s_23"
            END as "review_the_safety_planstep_5_professionals_or_professional_s_23",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_2_internal_coping_strategies_2' OR linkid = 'review_the_safety_planstep_2_internal_coping_strategies_2' OR short_name = 'review_the_safety_planstep_2_internal_coping_strategies_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_2_internal_coping_strategies_2"
            END as "review_the_safety_planstep_2_internal_coping_strategies_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_du_9' OR linkid = 'step_5professionals_or_professional_services_that_can_help_du_9' OR short_name = 'step_5professionals_or_professional_services_that_can_help_du_9')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_du_9"
            END as "step_5professionals_or_professional_services_that_can_help_du_9",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_s_24' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_s_24' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_s_24')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_s_24"
            END as "review_the_safety_planstep_5_professionals_or_professional_s_24",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_s_25' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_s_25' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_s_25')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_s_25"
            END as "review_the_safety_planstep_5_professionals_or_professional_s_25",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_2_internal_coping_strategies_3' OR linkid = 'review_the_safety_planstep_2_internal_coping_strategies_3' OR short_name = 'review_the_safety_planstep_2_internal_coping_strategies_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_2_internal_coping_strategies_3"
            END as "review_the_safety_planstep_2_internal_coping_strategies_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_4family_members_or_friends_who_may_offer_help_2' OR linkid = 'step_4family_members_or_friends_who_may_offer_help_2' OR short_name = 'step_4family_members_or_friends_who_may_offer_help_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_4family_members_or_friends_who_may_offer_help_2"
            END as "step_4family_members_or_friends_who_may_offer_help_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_person_1' OR linkid = 'spi_subform_4_person_1' OR short_name = 'spi_subform_4_person_1')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_person_1"
            END as "spi_subform_4_person_1",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_s_26' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_s_26' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_s_26')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_s_26"
            END as "review_the_safety_planstep_5_professionals_or_professional_s_26",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_1_warning_signs_4' OR linkid = 'review_the_safety_planstep_1_warning_signs_4' OR short_name = 'review_the_safety_planstep_1_warning_signs_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_1_warning_signs_4"
            END as "review_the_safety_planstep_1_warning_signs_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_person_5' OR linkid = 'spi_subform_4_person_5' OR short_name = 'spi_subform_4_person_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_person_5"
            END as "spi_subform_4_person_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_4family_members_or_friends_who_may_offer_help_3' OR linkid = 'step_4family_members_or_friends_who_may_offer_help_3' OR short_name = 'step_4family_members_or_friends_who_may_offer_help_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_4family_members_or_friends_who_may_offer_help_3"
            END as "step_4family_members_or_friends_who_may_offer_help_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_s_27' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_s_27' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_s_27')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_s_27"
            END as "review_the_safety_planstep_5_professionals_or_professional_s_27",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_item_9' OR linkid = 'spi_item_9' OR short_name = 'spi_item_9')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_item_9"
            END as "spi_item_9",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_4_family_members_or_friends_who_ma_4' OR linkid = 'review_the_safety_planstep_4_family_members_or_friends_who_ma_4' OR short_name = 'review_the_safety_planstep_4_family_members_or_friends_who_ma_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_4_family_members_or_friends_who_ma_4"
            END as "review_the_safety_planstep_4_family_members_or_friends_who_ma_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_using_a_firearm_i_will_protect_myself_from_this_r' OR linkid = 'spi_subform_4_using_a_firearm_i_will_protect_myself_from_this_r' OR short_name = 'spi_subform_4_using_a_firearm_i_will_protect_myself_from_this_r')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_using_a_firearm_i_will_protect_myself_from_this_r"
            END as "spi_subform_4_using_a_firearm_i_will_protect_myself_from_this_r",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_27' OR linkid = 'spi_subform_4_27' OR short_name = 'spi_subform_4_27')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_27"
            END as "spi_subform_4_27",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_28' OR linkid = 'spi_subform_4_28' OR short_name = 'spi_subform_4_28')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_28"
            END as "spi_subform_4_28",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_taskid_my_safety_plan_pdf' OR linkid = 'spi_subform_4_taskid_my_safety_plan_pdf' OR short_name = 'spi_subform_4_taskid_my_safety_plan_pdf')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_taskid_my_safety_plan_pdf"
            END as "spi_subform_4_taskid_my_safety_plan_pdf",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_d_10' OR linkid = 'step_5professionals_or_professional_services_that_can_help_d_10' OR short_name = 'step_5professionals_or_professional_services_that_can_help_d_10')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_d_10"
            END as "step_5professionals_or_professional_services_that_can_help_d_10",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_29' OR linkid = 'spi_subform_4_29' OR short_name = 'spi_subform_4_29')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_29"
            END as "spi_subform_4_29",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_s_28' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_s_28' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_s_28')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_s_28"
            END as "review_the_safety_planstep_5_professionals_or_professional_s_28",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_30' OR linkid = 'spi_subform_4_30' OR short_name = 'spi_subform_4_30')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_30"
            END as "spi_subform_4_30",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_31' OR linkid = 'spi_subform_4_31' OR short_name = 'spi_subform_4_31')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_31"
            END as "spi_subform_4_31",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_d_11' OR linkid = 'step_5professionals_or_professional_services_that_can_help_d_11' OR short_name = 'step_5professionals_or_professional_services_that_can_help_d_11')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_d_11"
            END as "step_5professionals_or_professional_services_that_can_help_d_11",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_32' OR linkid = 'spi_subform_4_32' OR short_name = 'spi_subform_4_32')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_32"
            END as "spi_subform_4_32",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_4_family_members_or_friends_who_ma_5' OR linkid = 'review_the_safety_planstep_4_family_members_or_friends_who_ma_5' OR short_name = 'review_the_safety_planstep_4_family_members_or_friends_who_ma_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_4_family_members_or_friends_who_ma_5"
            END as "review_the_safety_planstep_4_family_members_or_friends_who_ma_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_item_10' OR linkid = 'spi_item_10' OR short_name = 'spi_item_10')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_item_10"
            END as "spi_item_10",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_4_family_members_or_friends_who_ma_6' OR linkid = 'review_the_safety_planstep_4_family_members_or_friends_who_ma_6' OR short_name = 'review_the_safety_planstep_4_family_members_or_friends_who_ma_6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_4_family_members_or_friends_who_ma_6"
            END as "review_the_safety_planstep_4_family_members_or_friends_who_ma_6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_s_29' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_s_29' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_s_29')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_s_29"
            END as "review_the_safety_planstep_5_professionals_or_professional_s_29",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_33' OR linkid = 'spi_subform_4_33' OR short_name = 'spi_subform_4_33')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_33"
            END as "spi_subform_4_33",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_d_12' OR linkid = 'step_5professionals_or_professional_services_that_can_help_d_12' OR short_name = 'step_5professionals_or_professional_services_that_can_help_d_12')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_d_12"
            END as "step_5professionals_or_professional_services_that_can_help_d_12",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'when_you_feel_a_suicidal_crisis_arising_go_through_each_step_of' OR linkid = 'when_you_feel_a_suicidal_crisis_arising_go_through_each_step_of' OR short_name = 'when_you_feel_a_suicidal_crisis_arising_go_through_each_step_of')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "when_you_feel_a_suicidal_crisis_arising_go_through_each_step_of"
            END as "when_you_feel_a_suicidal_crisis_arising_go_through_each_step_of",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_1_warning_signs_5' OR linkid = 'review_the_safety_planstep_1_warning_signs_5' OR short_name = 'review_the_safety_planstep_1_warning_signs_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_1_warning_signs_5"
            END as "review_the_safety_planstep_1_warning_signs_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_d_13' OR linkid = 'step_5professionals_or_professional_services_that_can_help_d_13' OR short_name = 'step_5professionals_or_professional_services_that_can_help_d_13')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_d_13"
            END as "step_5professionals_or_professional_services_that_can_help_d_13",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_34' OR linkid = 'spi_subform_4_34' OR short_name = 'spi_subform_4_34')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_34"
            END as "spi_subform_4_34",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_d_14' OR linkid = 'step_5professionals_or_professional_services_that_can_help_d_14' OR short_name = 'step_5professionals_or_professional_services_that_can_help_d_14')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_d_14"
            END as "step_5professionals_or_professional_services_that_can_help_d_14",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_person_1_2' OR linkid = 'spi_subform_4_person_1_2' OR short_name = 'spi_subform_4_person_1_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_person_1_2"
            END as "spi_subform_4_person_1_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_taskid_spi_navigator' OR linkid = 'spi_subform_4_taskid_spi_navigator' OR short_name = 'spi_subform_4_taskid_spi_navigator')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_taskid_spi_navigator"
            END as "spi_subform_4_taskid_spi_navigator",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_35' OR linkid = 'spi_subform_4_35' OR short_name = 'spi_subform_4_35')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_35"
            END as "spi_subform_4_35",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_person_4_2' OR linkid = 'spi_subform_4_person_4_2' OR short_name = 'spi_subform_4_person_4_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_person_4_2"
            END as "spi_subform_4_person_4_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_item_11' OR linkid = 'spi_item_11' OR short_name = 'spi_item_11')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_item_11"
            END as "spi_item_11",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_s_30' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_s_30' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_s_30')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_s_30"
            END as "review_the_safety_planstep_5_professionals_or_professional_s_30",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_person_5_2' OR linkid = 'spi_subform_4_person_5_2' OR short_name = 'spi_subform_4_person_5_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_person_5_2"
            END as "spi_subform_4_person_5_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_6_making_the_environment_safe_taking' OR linkid = 'review_the_safety_planstep_6_making_the_environment_safe_taking' OR short_name = 'review_the_safety_planstep_6_making_the_environment_safe_taking')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_6_making_the_environment_safe_taking"
            END as "review_the_safety_planstep_6_making_the_environment_safe_taking",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_d_15' OR linkid = 'step_5professionals_or_professional_services_that_can_help_d_15' OR short_name = 'step_5professionals_or_professional_services_that_can_help_d_15')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_d_15"
            END as "step_5professionals_or_professional_services_that_can_help_d_15",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_36' OR linkid = 'spi_subform_4_36' OR short_name = 'spi_subform_4_36')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_36"
            END as "spi_subform_4_36",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_2_internal_coping_strategies_4' OR linkid = 'review_the_safety_planstep_2_internal_coping_strategies_4' OR short_name = 'review_the_safety_planstep_2_internal_coping_strategies_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_2_internal_coping_strategies_4"
            END as "review_the_safety_planstep_2_internal_coping_strategies_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_i_would_like_to_make_a_plan_that_will_help_you_ma' OR linkid = 'spi_subform_4_i_would_like_to_make_a_plan_that_will_help_you_ma' OR short_name = 'spi_subform_4_i_would_like_to_make_a_plan_that_will_help_you_ma')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_i_would_like_to_make_a_plan_that_will_help_you_ma"
            END as "spi_subform_4_i_would_like_to_make_a_plan_that_will_help_you_ma",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_4_family_members_or_friends_who_ma_7' OR linkid = 'review_the_safety_planstep_4_family_members_or_friends_who_ma_7' OR short_name = 'review_the_safety_planstep_4_family_members_or_friends_who_ma_7')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_4_family_members_or_friends_who_ma_7"
            END as "review_the_safety_planstep_4_family_members_or_friends_who_ma_7",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_s_31' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_s_31' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_s_31')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_s_31"
            END as "review_the_safety_planstep_5_professionals_or_professional_s_31",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_d_16' OR linkid = 'step_5professionals_or_professional_services_that_can_help_d_16' OR short_name = 'step_5professionals_or_professional_services_that_can_help_d_16')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_d_16"
            END as "step_5professionals_or_professional_services_that_can_help_d_16",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_s_32' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_s_32' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_s_32')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_s_32"
            END as "review_the_safety_planstep_5_professionals_or_professional_s_32",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_age_9' OR linkid = 'add_family_member_registration_calculated_age_9' OR short_name = 'add_family_member_registration_calculated_age_9')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_age_9"
            END as "add_family_member_registration_calculated_age_9",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_item_12' OR linkid = 'spi_item_12' OR short_name = 'spi_item_12')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_item_12"
            END as "spi_item_12",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_4_family_members_or_friends_who_ma_8' OR linkid = 'review_the_safety_planstep_4_family_members_or_friends_who_ma_8' OR short_name = 'review_the_safety_planstep_4_family_members_or_friends_who_ma_8')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_4_family_members_or_friends_who_ma_8"
            END as "review_the_safety_planstep_4_family_members_or_friends_who_ma_8",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_item_13' OR linkid = 'spi_item_13' OR short_name = 'spi_item_13')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_item_13"
            END as "spi_item_13",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_37' OR linkid = 'spi_subform_4_37' OR short_name = 'spi_subform_4_37')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_37"
            END as "spi_subform_4_37",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_4family_members_or_friends_who_may_offer_help_4' OR linkid = 'step_4family_members_or_friends_who_may_offer_help_4' OR short_name = 'step_4family_members_or_friends_who_may_offer_help_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_4family_members_or_friends_who_may_offer_help_4"
            END as "step_4family_members_or_friends_who_may_offer_help_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_38' OR linkid = 'spi_subform_4_38' OR short_name = 'spi_subform_4_38')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_38"
            END as "spi_subform_4_38",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_39' OR linkid = 'spi_subform_4_39' OR short_name = 'spi_subform_4_39')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_39"
            END as "spi_subform_4_39",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_d_17' OR linkid = 'step_5professionals_or_professional_services_that_can_help_d_17' OR short_name = 'step_5professionals_or_professional_services_that_can_help_d_17')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_d_17"
            END as "step_5professionals_or_professional_services_that_can_help_d_17",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_40' OR linkid = 'spi_subform_4_40' OR short_name = 'spi_subform_4_40')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_40"
            END as "spi_subform_4_40",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_d_18' OR linkid = 'step_5professionals_or_professional_services_that_can_help_d_18' OR short_name = 'step_5professionals_or_professional_services_that_can_help_d_18')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_d_18"
            END as "step_5professionals_or_professional_services_that_can_help_d_18",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_4_family_members_or_friends_who_ma_9' OR linkid = 'review_the_safety_planstep_4_family_members_or_friends_who_ma_9' OR short_name = 'review_the_safety_planstep_4_family_members_or_friends_who_ma_9')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_4_family_members_or_friends_who_ma_9"
            END as "review_the_safety_planstep_4_family_members_or_friends_who_ma_9",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_d_19' OR linkid = 'step_5professionals_or_professional_services_that_can_help_d_19' OR short_name = 'step_5professionals_or_professional_services_that_can_help_d_19')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_d_19"
            END as "step_5professionals_or_professional_services_that_can_help_d_19",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_4family_members_or_friends_who_may_offer_help_are_you_wi_3' OR linkid = 'step_4family_members_or_friends_who_may_offer_help_are_you_wi_3' OR short_name = 'step_4family_members_or_friends_who_may_offer_help_are_you_wi_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_4family_members_or_friends_who_may_offer_help_are_you_wi_3"
            END as "step_4family_members_or_friends_who_may_offer_help_are_you_wi_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_41' OR linkid = 'spi_subform_4_41' OR short_name = 'spi_subform_4_41')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_41"
            END as "spi_subform_4_41",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_42' OR linkid = 'spi_subform_4_42' OR short_name = 'spi_subform_4_42')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_42"
            END as "spi_subform_4_42",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_other_ways_i_will_make_my_environment_safer' OR linkid = 'spi_subform_4_other_ways_i_will_make_my_environment_safer' OR short_name = 'spi_subform_4_other_ways_i_will_make_my_environment_safer')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_other_ways_i_will_make_my_environment_safer"
            END as "spi_subform_4_other_ways_i_will_make_my_environment_safer",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_43' OR linkid = 'spi_subform_4_43' OR short_name = 'spi_subform_4_43')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_43"
            END as "spi_subform_4_43",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_d_20' OR linkid = 'step_5professionals_or_professional_services_that_can_help_d_20' OR short_name = 'step_5professionals_or_professional_services_that_can_help_d_20')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_d_20"
            END as "step_5professionals_or_professional_services_that_can_help_d_20",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_d_21' OR linkid = 'step_5professionals_or_professional_services_that_can_help_d_21' OR short_name = 'step_5professionals_or_professional_services_that_can_help_d_21')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_d_21"
            END as "step_5professionals_or_professional_services_that_can_help_d_21",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_month_9' OR linkid = 'add_family_member_registration_calculated_month_9' OR short_name = 'add_family_member_registration_calculated_month_9')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_month_9"
            END as "add_family_member_registration_calculated_month_9",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_year_9' OR linkid = 'add_family_member_registration_calculated_year_9' OR short_name = 'add_family_member_registration_calculated_year_9')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_year_9"
            END as "add_family_member_registration_calculated_year_9",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_d_22' OR linkid = 'step_5professionals_or_professional_services_that_can_help_d_22' OR short_name = 'step_5professionals_or_professional_services_that_can_help_d_22')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_d_22"
            END as "step_5professionals_or_professional_services_that_can_help_d_22",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_2_internal_coping_strategies_5' OR linkid = 'review_the_safety_planstep_2_internal_coping_strategies_5' OR short_name = 'review_the_safety_planstep_2_internal_coping_strategies_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_2_internal_coping_strategies_5"
            END as "review_the_safety_planstep_2_internal_coping_strategies_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_date_of_birth_9' OR linkid = 'add_family_member_registration_date_of_birth_9' OR short_name = 'add_family_member_registration_date_of_birth_9')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_date_of_birth_9"
            END as "add_family_member_registration_date_of_birth_9",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_44' OR linkid = 'spi_subform_4_44' OR short_name = 'spi_subform_4_44')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_44"
            END as "spi_subform_4_44",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_d_23' OR linkid = 'step_5professionals_or_professional_services_that_can_help_d_23' OR short_name = 'step_5professionals_or_professional_services_that_can_help_d_23')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_d_23"
            END as "step_5professionals_or_professional_services_that_can_help_d_23",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_4_family_members_or_friends_who_m_10' OR linkid = 'review_the_safety_planstep_4_family_members_or_friends_who_m_10' OR short_name = 'review_the_safety_planstep_4_family_members_or_friends_who_m_10')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_4_family_members_or_friends_who_m_10"
            END as "review_the_safety_planstep_4_family_members_or_friends_who_m_10",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_d_24' OR linkid = 'step_5professionals_or_professional_services_that_can_help_d_24' OR short_name = 'step_5professionals_or_professional_services_that_can_help_d_24')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_d_24"
            END as "step_5professionals_or_professional_services_that_can_help_d_24",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_6_making_the_environment_safe_going_' OR linkid = 'review_the_safety_planstep_6_making_the_environment_safe_going_' OR short_name = 'review_the_safety_planstep_6_making_the_environment_safe_going_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_6_making_the_environment_safe_going_"
            END as "review_the_safety_planstep_6_making_the_environment_safe_going_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_item_14' OR linkid = 'spi_item_14' OR short_name = 'spi_item_14')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_item_14"
            END as "spi_item_14",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_d_25' OR linkid = 'step_5professionals_or_professional_services_that_can_help_d_25' OR short_name = 'step_5professionals_or_professional_services_that_can_help_d_25')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_d_25"
            END as "step_5professionals_or_professional_services_that_can_help_d_25",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_4_family_members_or_friends_who_m_11' OR linkid = 'review_the_safety_planstep_4_family_members_or_friends_who_m_11' OR short_name = 'review_the_safety_planstep_4_family_members_or_friends_who_m_11')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_4_family_members_or_friends_who_m_11"
            END as "review_the_safety_planstep_4_family_members_or_friends_who_m_11",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_45' OR linkid = 'spi_subform_4_45' OR short_name = 'spi_subform_4_45')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_45"
            END as "spi_subform_4_45",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_person_2_2' OR linkid = 'spi_subform_4_person_2_2' OR short_name = 'spi_subform_4_person_2_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_person_2_2"
            END as "spi_subform_4_person_2_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_4family_members_or_friends_who_may_offer_help_person_1' OR linkid = 'step_4family_members_or_friends_who_may_offer_help_person_1' OR short_name = 'step_4family_members_or_friends_who_may_offer_help_person_1')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_4family_members_or_friends_who_may_offer_help_person_1"
            END as "step_4family_members_or_friends_who_may_offer_help_person_1",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_4_family_members_or_friends_who_m_12' OR linkid = 'review_the_safety_planstep_4_family_members_or_friends_who_m_12' OR short_name = 'review_the_safety_planstep_4_family_members_or_friends_who_m_12')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_4_family_members_or_friends_who_m_12"
            END as "review_the_safety_planstep_4_family_members_or_friends_who_m_12",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_6_making_the_environment_safe_using_' OR linkid = 'review_the_safety_planstep_6_making_the_environment_safe_using_' OR short_name = 'review_the_safety_planstep_6_making_the_environment_safe_using_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_6_making_the_environment_safe_using_"
            END as "review_the_safety_planstep_6_making_the_environment_safe_using_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_46' OR linkid = 'spi_subform_4_46' OR short_name = 'spi_subform_4_46')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_46"
            END as "spi_subform_4_46",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_s_33' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_s_33' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_s_33')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_s_33"
            END as "review_the_safety_planstep_5_professionals_or_professional_s_33",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_d_26' OR linkid = 'step_5professionals_or_professional_services_that_can_help_d_26' OR short_name = 'step_5professionals_or_professional_services_that_can_help_d_26')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_d_26"
            END as "step_5professionals_or_professional_services_that_can_help_d_26",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_d_27' OR linkid = 'step_5professionals_or_professional_services_that_can_help_d_27' OR short_name = 'step_5professionals_or_professional_services_that_can_help_d_27')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_d_27"
            END as "step_5professionals_or_professional_services_that_can_help_d_27",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_d_28' OR linkid = 'step_5professionals_or_professional_services_that_can_help_d_28' OR short_name = 'step_5professionals_or_professional_services_that_can_help_d_28')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_d_28"
            END as "step_5professionals_or_professional_services_that_can_help_d_28",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_d_29' OR linkid = 'step_5professionals_or_professional_services_that_can_help_d_29' OR short_name = 'step_5professionals_or_professional_services_that_can_help_d_29')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_d_29"
            END as "step_5professionals_or_professional_services_that_can_help_d_29",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_item_15' OR linkid = 'spi_item_15' OR short_name = 'spi_item_15')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_item_15"
            END as "spi_item_15",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_47' OR linkid = 'spi_subform_4_47' OR short_name = 'spi_subform_4_47')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_47"
            END as "spi_subform_4_47",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_6_making_the_environment_safe_hangin' OR linkid = 'review_the_safety_planstep_6_making_the_environment_safe_hangin' OR short_name = 'review_the_safety_planstep_6_making_the_environment_safe_hangin')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_6_making_the_environment_safe_hangin"
            END as "review_the_safety_planstep_6_making_the_environment_safe_hangin",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_item_16' OR linkid = 'spi_item_16' OR short_name = 'spi_item_16')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_item_16"
            END as "spi_item_16",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_s_34' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_s_34' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_s_34')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_s_34"
            END as "review_the_safety_planstep_5_professionals_or_professional_s_34",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_4_family_members_or_friends_who_m_13' OR linkid = 'review_the_safety_planstep_4_family_members_or_friends_who_m_13' OR short_name = 'review_the_safety_planstep_4_family_members_or_friends_who_m_13')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_4_family_members_or_friends_who_m_13"
            END as "review_the_safety_planstep_4_family_members_or_friends_who_m_13",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_item_17' OR linkid = 'spi_item_17' OR short_name = 'spi_item_17')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_item_17"
            END as "spi_item_17",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_48' OR linkid = 'spi_subform_4_48' OR short_name = 'spi_subform_4_48')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_48"
            END as "spi_subform_4_48",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_49' OR linkid = 'spi_subform_4_49' OR short_name = 'spi_subform_4_49')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_49"
            END as "spi_subform_4_49",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_d_30' OR linkid = 'step_5professionals_or_professional_services_that_can_help_d_30' OR short_name = 'step_5professionals_or_professional_services_that_can_help_d_30')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_d_30"
            END as "step_5professionals_or_professional_services_that_can_help_d_30",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_d_31' OR linkid = 'step_5professionals_or_professional_services_that_can_help_d_31' OR short_name = 'step_5professionals_or_professional_services_that_can_help_d_31')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_d_31"
            END as "step_5professionals_or_professional_services_that_can_help_d_31",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_4_family_members_or_friends_who_m_14' OR linkid = 'review_the_safety_planstep_4_family_members_or_friends_who_m_14' OR short_name = 'review_the_safety_planstep_4_family_members_or_friends_who_m_14')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_4_family_members_or_friends_who_m_14"
            END as "review_the_safety_planstep_4_family_members_or_friends_who_m_14",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_4family_members_or_friends_who_may_offer_help_person_3' OR linkid = 'step_4family_members_or_friends_who_may_offer_help_person_3' OR short_name = 'step_4family_members_or_friends_who_may_offer_help_person_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_4family_members_or_friends_who_may_offer_help_person_3"
            END as "step_4family_members_or_friends_who_may_offer_help_person_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_6_making_the_environment_safe_drowni' OR linkid = 'review_the_safety_planstep_6_making_the_environment_safe_drowni' OR short_name = 'review_the_safety_planstep_6_making_the_environment_safe_drowni')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_6_making_the_environment_safe_drowni"
            END as "review_the_safety_planstep_6_making_the_environment_safe_drowni",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_4_family_members_or_friends_who_m_15' OR linkid = 'review_the_safety_planstep_4_family_members_or_friends_who_m_15' OR short_name = 'review_the_safety_planstep_4_family_members_or_friends_who_m_15')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_4_family_members_or_friends_who_m_15"
            END as "review_the_safety_planstep_4_family_members_or_friends_who_m_15",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_5_professionals_or_professional_s_35' OR linkid = 'review_the_safety_planstep_5_professionals_or_professional_s_35' OR short_name = 'review_the_safety_planstep_5_professionals_or_professional_s_35')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_5_professionals_or_professional_s_35"
            END as "review_the_safety_planstep_5_professionals_or_professional_s_35",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_50' OR linkid = 'spi_subform_4_50' OR short_name = 'spi_subform_4_50')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_50"
            END as "spi_subform_4_50",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_68' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_68' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_68')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_68"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_68",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_6_making_the_environment_safe_other_' OR linkid = 'review_the_safety_planstep_6_making_the_environment_safe_other_' OR short_name = 'review_the_safety_planstep_6_making_the_environment_safe_other_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_6_making_the_environment_safe_other_"
            END as "review_the_safety_planstep_6_making_the_environment_safe_other_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_51' OR linkid = 'spi_subform_4_51' OR short_name = 'spi_subform_4_51')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_51"
            END as "spi_subform_4_51",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_d_32' OR linkid = 'step_5professionals_or_professional_services_that_can_help_d_32' OR short_name = 'step_5professionals_or_professional_services_that_can_help_d_32')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_d_32"
            END as "step_5professionals_or_professional_services_that_can_help_d_32",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_4_family_members_or_friends_who_m_16' OR linkid = 'review_the_safety_planstep_4_family_members_or_friends_who_m_16' OR short_name = 'review_the_safety_planstep_4_family_members_or_friends_who_m_16')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_4_family_members_or_friends_who_m_16"
            END as "review_the_safety_planstep_4_family_members_or_friends_who_m_16",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_person_3_2' OR linkid = 'spi_subform_4_person_3_2' OR short_name = 'spi_subform_4_person_3_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_person_3_2"
            END as "spi_subform_4_person_3_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_d_33' OR linkid = 'step_5professionals_or_professional_services_that_can_help_d_33' OR short_name = 'step_5professionals_or_professional_services_that_can_help_d_33')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_d_33"
            END as "step_5professionals_or_professional_services_that_can_help_d_33",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_your_local_hotline_number' OR linkid = 'spi_subform_4_your_local_hotline_number' OR short_name = 'spi_subform_4_your_local_hotline_number')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_your_local_hotline_number"
            END as "spi_subform_4_your_local_hotline_number",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_52' OR linkid = 'spi_subform_4_52' OR short_name = 'spi_subform_4_52')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_52"
            END as "spi_subform_4_52",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_d_34' OR linkid = 'step_5professionals_or_professional_services_that_can_help_d_34' OR short_name = 'step_5professionals_or_professional_services_that_can_help_d_34')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_d_34"
            END as "step_5professionals_or_professional_services_that_can_help_d_34",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_5professionals_or_professional_services_that_can_help_d_35' OR linkid = 'step_5professionals_or_professional_services_that_can_help_d_35' OR short_name = 'step_5professionals_or_professional_services_that_can_help_d_35')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_5professionals_or_professional_services_that_can_help_d_35"
            END as "step_5professionals_or_professional_services_that_can_help_d_35",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_53' OR linkid = 'spi_subform_4_53' OR short_name = 'spi_subform_4_53')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_53"
            END as "spi_subform_4_53",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_additional_hotline_numbers_you_can_contact' OR linkid = 'spi_subform_4_additional_hotline_numbers_you_can_contact' OR short_name = 'spi_subform_4_additional_hotline_numbers_you_can_contact')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_additional_hotline_numbers_you_can_contact"
            END as "spi_subform_4_additional_hotline_numbers_you_can_contact",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_54' OR linkid = 'spi_subform_4_54' OR short_name = 'spi_subform_4_54')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_54"
            END as "spi_subform_4_54",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_going_in_front_of_a_moving_vehicle_i_will_protect' OR linkid = 'spi_subform_4_going_in_front_of_a_moving_vehicle_i_will_protect' OR short_name = 'spi_subform_4_going_in_front_of_a_moving_vehicle_i_will_protect')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_going_in_front_of_a_moving_vehicle_i_will_protect"
            END as "spi_subform_4_going_in_front_of_a_moving_vehicle_i_will_protect",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'review_the_safety_planstep_4_family_members_or_friends_who_m_17' OR linkid = 'review_the_safety_planstep_4_family_members_or_friends_who_m_17' OR short_name = 'review_the_safety_planstep_4_family_members_or_friends_who_m_17')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "review_the_safety_planstep_4_family_members_or_friends_who_m_17"
            END as "review_the_safety_planstep_4_family_members_or_friends_who_m_17",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_55' OR linkid = 'spi_subform_4_55' OR short_name = 'spi_subform_4_55')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_55"
            END as "spi_subform_4_55",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_4family_members_or_friends_who_may_offer_help_5' OR linkid = 'step_4family_members_or_friends_who_may_offer_help_5' OR short_name = 'step_4family_members_or_friends_who_may_offer_help_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_4family_members_or_friends_who_may_offer_help_5"
            END as "step_4family_members_or_friends_who_may_offer_help_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'step_4family_members_or_friends_who_may_offer_help_6' OR linkid = 'step_4family_members_or_friends_who_may_offer_help_6' OR short_name = 'step_4family_members_or_friends_who_may_offer_help_6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "step_4family_members_or_friends_who_may_offer_help_6"
            END as "step_4family_members_or_friends_who_may_offer_help_6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_56' OR linkid = 'spi_subform_4_56' OR short_name = 'spi_subform_4_56')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_56"
            END as "spi_subform_4_56",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'no_response_step_1_review' OR linkid = 'no_response_step_1_review' OR short_name = 'no_response_step_1_review')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "no_response_step_1_review"
            END as "no_response_step_1_review",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_57' OR linkid = 'spi_subform_4_57' OR short_name = 'spi_subform_4_57')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_57"
            END as "spi_subform_4_57",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'no_response_step_2_review' OR linkid = 'no_response_step_2_review' OR short_name = 'no_response_step_2_review')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "no_response_step_2_review"
            END as "no_response_step_2_review",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_58' OR linkid = 'spi_subform_4_58' OR short_name = 'spi_subform_4_58')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_58"
            END as "spi_subform_4_58",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'no_response_step_3_review' OR linkid = 'no_response_step_3_review' OR short_name = 'no_response_step_3_review')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "no_response_step_3_review"
            END as "no_response_step_3_review",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_59' OR linkid = 'spi_subform_4_59' OR short_name = 'spi_subform_4_59')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_59"
            END as "spi_subform_4_59",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'no_response_step_4_review' OR linkid = 'no_response_step_4_review' OR short_name = 'no_response_step_4_review')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "no_response_step_4_review"
            END as "no_response_step_4_review",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_60' OR linkid = 'spi_subform_4_60' OR short_name = 'spi_subform_4_60')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_60"
            END as "spi_subform_4_60",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'no_response_step_6a_review' OR linkid = 'no_response_step_6a_review' OR short_name = 'no_response_step_6a_review')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "no_response_step_6a_review"
            END as "no_response_step_6a_review",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_4_61' OR linkid = 'spi_subform_4_61' OR short_name = 'spi_subform_4_61')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_4_61"
            END as "spi_subform_4_61",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data

