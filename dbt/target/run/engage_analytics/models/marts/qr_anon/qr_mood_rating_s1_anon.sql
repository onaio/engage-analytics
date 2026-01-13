
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_mood_rating_s1_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_mood_rating_s1
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_mood_rating_s1'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_mood_rating_s1"
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
                    WHERE ("column" = 'mood_rating_ipc_session_1' OR linkid = 'mood_rating_ipc_session_1' OR short_name = 'mood_rating_ipc_session_1')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mood_rating_ipc_session_1"
            END as "mood_rating_ipc_session_1",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_age_3' OR linkid = 'add_family_member_registration_calculated_age_3' OR short_name = 'add_family_member_registration_calculated_age_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_age_3"
            END as "add_family_member_registration_calculated_age_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_month_3' OR linkid = 'add_family_member_registration_calculated_month_3' OR short_name = 'add_family_member_registration_calculated_month_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_month_3"
            END as "add_family_member_registration_calculated_month_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_year_3' OR linkid = 'add_family_member_registration_calculated_year_3' OR short_name = 'add_family_member_registration_calculated_year_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_year_3"
            END as "add_family_member_registration_calculated_year_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_date_of_birth_3' OR linkid = 'add_family_member_registration_date_of_birth_3' OR short_name = 'add_family_member_registration_date_of_birth_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_date_of_birth_3"
            END as "add_family_member_registration_date_of_birth_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mood_rating_ipc_session_1_total_score' OR linkid = 'mood_rating_ipc_session_1_total_score' OR short_name = 'mood_rating_ipc_session_1_total_score')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mood_rating_ipc_session_1_total_score"
            END as "mood_rating_ipc_session_1_total_score",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being_th' OR linkid = 'mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being_th' OR short_name = 'mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being_th')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being_th"
            END as "mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being_th",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'LINK_ID_THAT_CONTAINS_ENCOUNTER_ID_AS_ANSWER' OR linkid = 'LINK_ID_THAT_CONTAINS_ENCOUNTER_ID_AS_ANSWER' OR short_name = 'LINK_ID_THAT_CONTAINS_ENCOUNTER_ID_AS_ANSWER')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "LINK_ID_THAT_CONTAINS_ENCOUNTER_ID_AS_ANSWER"
            END as "LINK_ID_THAT_CONTAINS_ENCOUNTER_ID_AS_ANSWER",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data


  );