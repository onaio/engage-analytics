

-- Anonymized view for qr_phq_9_s3
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_phq_9_s3'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_phq_9_s3"
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
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_37' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_37' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_37')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_37"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_37",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_38' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_38' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_38')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_38"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_38",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_39' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_39' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_39')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_39"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_39",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_moving_or_speaking_so_slowly_th_4' OR linkid = 'common_mental_health_symptoms_moving_or_speaking_so_slowly_th_4' OR short_name = 'common_mental_health_symptoms_moving_or_speaking_so_slowly_th_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_moving_or_speaking_so_slowly_th_4"
            END as "common_mental_health_symptoms_moving_or_speaking_so_slowly_th_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_thoughts_that_you_would_be_bett_4' OR linkid = 'common_mental_health_symptoms_thoughts_that_you_would_be_bett_4' OR short_name = 'common_mental_health_symptoms_thoughts_that_you_would_be_bett_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_thoughts_that_you_would_be_bett_4"
            END as "common_mental_health_symptoms_thoughts_that_you_would_be_bett_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_little_interest_or_pleasure_in__4' OR linkid = 'common_mental_health_symptoms_little_interest_or_pleasure_in__4' OR short_name = 'common_mental_health_symptoms_little_interest_or_pleasure_in__4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_little_interest_or_pleasure_in__4"
            END as "common_mental_health_symptoms_little_interest_or_pleasure_in__4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_feeling_down_depressed_or_hopel_4' OR linkid = 'common_mental_health_symptoms_feeling_down_depressed_or_hopel_4' OR short_name = 'common_mental_health_symptoms_feeling_down_depressed_or_hopel_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_feeling_down_depressed_or_hopel_4"
            END as "common_mental_health_symptoms_feeling_down_depressed_or_hopel_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_trouble_falling_or_staying_asle_4' OR linkid = 'common_mental_health_symptoms_trouble_falling_or_staying_asle_4' OR short_name = 'common_mental_health_symptoms_trouble_falling_or_staying_asle_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_trouble_falling_or_staying_asle_4"
            END as "common_mental_health_symptoms_trouble_falling_or_staying_asle_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_feeling_tired_or_having_little__4' OR linkid = 'common_mental_health_symptoms_feeling_tired_or_having_little__4' OR short_name = 'common_mental_health_symptoms_feeling_tired_or_having_little__4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_feeling_tired_or_having_little__4"
            END as "common_mental_health_symptoms_feeling_tired_or_having_little__4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_poor_appetite_or_overeating_4' OR linkid = 'common_mental_health_symptoms_poor_appetite_or_overeating_4' OR short_name = 'common_mental_health_symptoms_poor_appetite_or_overeating_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_poor_appetite_or_overeating_4"
            END as "common_mental_health_symptoms_poor_appetite_or_overeating_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_feeling_bad_about_yourself_or_t_4' OR linkid = 'common_mental_health_symptoms_feeling_bad_about_yourself_or_t_4' OR short_name = 'common_mental_health_symptoms_feeling_bad_about_yourself_or_t_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_feeling_bad_about_yourself_or_t_4"
            END as "common_mental_health_symptoms_feeling_bad_about_yourself_or_t_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_trouble_concentrating_on_things_4' OR linkid = 'common_mental_health_symptoms_trouble_concentrating_on_things_4' OR short_name = 'common_mental_health_symptoms_trouble_concentrating_on_things_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_trouble_concentrating_on_things_4"
            END as "common_mental_health_symptoms_trouble_concentrating_on_things_4",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data

