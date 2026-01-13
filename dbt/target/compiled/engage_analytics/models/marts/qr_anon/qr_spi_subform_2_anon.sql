

-- Anonymized view for qr_spi_subform_2
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_spi_subform_2'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_spi_subform_2"
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
                    WHERE ("column" = 'spi_subform_2_did_this_client_also_screen_positive_for_probable' OR linkid = 'spi_subform_2_did_this_client_also_screen_positive_for_probable' OR short_name = 'spi_subform_2_did_this_client_also_screen_positive_for_probable')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_2_did_this_client_also_screen_positive_for_probable"
            END as "spi_subform_2_did_this_client_also_screen_positive_for_probable",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_2_did_you_discuss_this_client_s_severe_mental_healt' OR linkid = 'spi_subform_2_did_you_discuss_this_client_s_severe_mental_healt' OR short_name = 'spi_subform_2_did_you_discuss_this_client_s_severe_mental_healt')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_2_did_you_discuss_this_client_s_severe_mental_healt"
            END as "spi_subform_2_did_you_discuss_this_client_s_severe_mental_healt",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_2_this_client_scored_in_the_moderate_to_high_range_' OR linkid = 'spi_subform_2_this_client_scored_in_the_moderate_to_high_range_' OR short_name = 'spi_subform_2_this_client_scored_in_the_moderate_to_high_range_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_2_this_client_scored_in_the_moderate_to_high_range_"
            END as "spi_subform_2_this_client_scored_in_the_moderate_to_high_range_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_2_what_is_the_recommended_plan_to_address_the_proba' OR linkid = 'spi_subform_2_what_is_the_recommended_plan_to_address_the_proba' OR short_name = 'spi_subform_2_what_is_the_recommended_plan_to_address_the_proba')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_2_what_is_the_recommended_plan_to_address_the_proba"
            END as "spi_subform_2_what_is_the_recommended_plan_to_address_the_proba",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_2' OR linkid = 'spi_subform_2' OR short_name = 'spi_subform_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_2"
            END as "spi_subform_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_2_2' OR linkid = 'spi_subform_2_2' OR short_name = 'spi_subform_2_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_2_2"
            END as "spi_subform_2_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_2_3' OR linkid = 'spi_subform_2_3' OR short_name = 'spi_subform_2_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_2_3"
            END as "spi_subform_2_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_2_what_is_the_recommended_plan_to_address_the_pro_2' OR linkid = 'spi_subform_2_what_is_the_recommended_plan_to_address_the_pro_2' OR short_name = 'spi_subform_2_what_is_the_recommended_plan_to_address_the_pro_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_2_what_is_the_recommended_plan_to_address_the_pro_2"
            END as "spi_subform_2_what_is_the_recommended_plan_to_address_the_pro_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_66' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_66' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_66')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_66"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_66",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_2_did_you_discuss_this_client_s_suicide_risk_screen' OR linkid = 'spi_subform_2_did_you_discuss_this_client_s_suicide_risk_screen' OR short_name = 'spi_subform_2_did_you_discuss_this_client_s_suicide_risk_screen')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_2_did_you_discuss_this_client_s_suicide_risk_screen"
            END as "spi_subform_2_did_you_discuss_this_client_s_suicide_risk_screen",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_2_taskid_spi_subform_3_2' OR linkid = 'spi_subform_2_taskid_spi_subform_3_2' OR short_name = 'spi_subform_2_taskid_spi_subform_3_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_2_taskid_spi_subform_3_2"
            END as "spi_subform_2_taskid_spi_subform_3_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_2_taskid_spi_subform_4_2' OR linkid = 'spi_subform_2_taskid_spi_subform_4_2' OR short_name = 'spi_subform_2_taskid_spi_subform_4_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_2_taskid_spi_subform_4_2"
            END as "spi_subform_2_taskid_spi_subform_4_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_2_taskstatus_spi_subform_3_2' OR linkid = 'spi_subform_2_taskstatus_spi_subform_3_2' OR short_name = 'spi_subform_2_taskstatus_spi_subform_3_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_2_taskstatus_spi_subform_3_2"
            END as "spi_subform_2_taskstatus_spi_subform_3_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_2_taskstatus_spi_subform_4_2' OR linkid = 'spi_subform_2_taskstatus_spi_subform_4_2' OR short_name = 'spi_subform_2_taskstatus_spi_subform_4_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_2_taskstatus_spi_subform_4_2"
            END as "spi_subform_2_taskstatus_spi_subform_4_2",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data

