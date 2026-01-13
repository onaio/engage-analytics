

-- Anonymized view for qr_remove_patient
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_remove_patient'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_remove_patient"
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
                    WHERE ("column" = 'ecbis_remove_family_form_date_of_death' OR linkid = 'ecbis_remove_family_form_date_of_death' OR short_name = 'ecbis_remove_family_form_date_of_death')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ecbis_remove_family_form_date_of_death"
            END as "ecbis_remove_family_form_date_of_death",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ecbis_remove_family_form_give_other_reasons_for_removal' OR linkid = 'ecbis_remove_family_form_give_other_reasons_for_removal' OR short_name = 'ecbis_remove_family_form_give_other_reasons_for_removal')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ecbis_remove_family_form_give_other_reasons_for_removal"
            END as "ecbis_remove_family_form_give_other_reasons_for_removal",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ecbis_remove_family_form_age_at_death_year' OR linkid = 'ecbis_remove_family_form_age_at_death_year' OR short_name = 'ecbis_remove_family_form_age_at_death_year')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ecbis_remove_family_form_age_at_death_year"
            END as "ecbis_remove_family_form_age_at_death_year",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ecbis_remove_family_form' OR linkid = 'ecbis_remove_family_form' OR short_name = 'ecbis_remove_family_form')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ecbis_remove_family_form"
            END as "ecbis_remove_family_form",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ecbis_remove_family_form_date_moved_away' OR linkid = 'ecbis_remove_family_form_date_moved_away' OR short_name = 'ecbis_remove_family_form_date_moved_away')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ecbis_remove_family_form_date_moved_away"
            END as "ecbis_remove_family_form_date_moved_away",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'survey_response' OR linkid = 'survey_response' OR short_name = 'survey_response')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "survey_response"
            END as "survey_response",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ecbis_remove_family_form_reason_for_removal' OR linkid = 'ecbis_remove_family_form_reason_for_removal' OR short_name = 'ecbis_remove_family_form_reason_for_removal')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ecbis_remove_family_form_reason_for_removal"
            END as "ecbis_remove_family_form_reason_for_removal",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'patient-birthdate' OR linkid = 'patient-birthdate' OR short_name = 'patient-birthdate')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "patient-birthdate"
            END as "patient-birthdate",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data

