
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_mood_rating_s2_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_mood_rating_s2
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_mood_rating_s2'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_mood_rating_s2"
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
                    WHERE ("column" = 'mood_rating_ipc_session_2_total_score' OR linkid = 'mood_rating_ipc_session_2_total_score' OR short_name = 'mood_rating_ipc_session_2_total_score')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mood_rating_ipc_session_2_total_score"
            END as "mood_rating_ipc_session_2_total_score",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_age_4' OR linkid = 'add_family_member_registration_calculated_age_4' OR short_name = 'add_family_member_registration_calculated_age_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_age_4"
            END as "add_family_member_registration_calculated_age_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_month_4' OR linkid = 'add_family_member_registration_calculated_month_4' OR short_name = 'add_family_member_registration_calculated_month_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_month_4"
            END as "add_family_member_registration_calculated_month_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_year_4' OR linkid = 'add_family_member_registration_calculated_year_4' OR short_name = 'add_family_member_registration_calculated_year_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_year_4"
            END as "add_family_member_registration_calculated_year_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_date_of_birth_4' OR linkid = 'add_family_member_registration_date_of_birth_4' OR short_name = 'add_family_member_registration_date_of_birth_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_date_of_birth_4"
            END as "add_family_member_registration_date_of_birth_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being__2' OR linkid = 'mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being__2' OR short_name = 'mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being__2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being__2"
            END as "mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being__2",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data


  );