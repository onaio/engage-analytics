

-- Anonymized view for qr_1_month_follow_up
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_1_month_follow_up'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_1_month_follow_up"
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
                    WHERE ("column" = 'one_month_follow_up_scheduling_have_you_completed_all_recommend' OR linkid = 'one_month_follow_up_scheduling_have_you_completed_all_recommend' OR short_name = 'one_month_follow_up_scheduling_have_you_completed_all_recommend')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "one_month_follow_up_scheduling_have_you_completed_all_recommend"
            END as "one_month_follow_up_scheduling_have_you_completed_all_recommend",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'describe_one_month_follow_up_let_s_schedule_this_meeting_now' OR linkid = 'describe_one_month_follow_up_let_s_schedule_this_meeting_now' OR short_name = 'describe_one_month_follow_up_let_s_schedule_this_meeting_now')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "describe_one_month_follow_up_let_s_schedule_this_meeting_now"
            END as "describe_one_month_follow_up_let_s_schedule_this_meeting_now",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data

