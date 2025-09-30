
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_confidential_s1_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_confidential_s1
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_confidential_s1'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_confidential_s1"
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
                    WHERE ("column" = 'add_family_member_registration_calculated_age' OR linkid = 'add_family_member_registration_calculated_age' OR short_name = 'add_family_member_registration_calculated_age')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_age"
            END as "add_family_member_registration_calculated_age",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_month' OR linkid = 'add_family_member_registration_calculated_month' OR short_name = 'add_family_member_registration_calculated_month')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_month"
            END as "add_family_member_registration_calculated_month",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_year' OR linkid = 'add_family_member_registration_calculated_year' OR short_name = 'add_family_member_registration_calculated_year')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_year"
            END as "add_family_member_registration_calculated_year",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_date_of_birth' OR linkid = 'add_family_member_registration_date_of_birth' OR short_name = 'add_family_member_registration_date_of_birth')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_date_of_birth"
            END as "add_family_member_registration_date_of_birth",
        
    
        
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