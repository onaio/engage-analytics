

-- Anonymized view for qr_phq_9_s2
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_phq_9_s2'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_phq_9_s2"
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
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_32' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_32' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_32')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_32"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_32",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_33' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_33' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_33')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_33"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_33",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_34' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_34' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_34')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_34"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_34",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_35' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_35' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_35')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_35"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_35",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_moving_or_speaking_so_slowly_th_3' OR linkid = 'common_mental_health_symptoms_moving_or_speaking_so_slowly_th_3' OR short_name = 'common_mental_health_symptoms_moving_or_speaking_so_slowly_th_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_moving_or_speaking_so_slowly_th_3"
            END as "common_mental_health_symptoms_moving_or_speaking_so_slowly_th_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_thoughts_that_you_would_be_bett_3' OR linkid = 'common_mental_health_symptoms_thoughts_that_you_would_be_bett_3' OR short_name = 'common_mental_health_symptoms_thoughts_that_you_would_be_bett_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_thoughts_that_you_would_be_bett_3"
            END as "common_mental_health_symptoms_thoughts_that_you_would_be_bett_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_little_interest_or_pleasure_in__3' OR linkid = 'common_mental_health_symptoms_little_interest_or_pleasure_in__3' OR short_name = 'common_mental_health_symptoms_little_interest_or_pleasure_in__3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_little_interest_or_pleasure_in__3"
            END as "common_mental_health_symptoms_little_interest_or_pleasure_in__3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_feeling_down_depressed_or_hopel_3' OR linkid = 'common_mental_health_symptoms_feeling_down_depressed_or_hopel_3' OR short_name = 'common_mental_health_symptoms_feeling_down_depressed_or_hopel_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_feeling_down_depressed_or_hopel_3"
            END as "common_mental_health_symptoms_feeling_down_depressed_or_hopel_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_trouble_falling_or_staying_asle_3' OR linkid = 'common_mental_health_symptoms_trouble_falling_or_staying_asle_3' OR short_name = 'common_mental_health_symptoms_trouble_falling_or_staying_asle_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_trouble_falling_or_staying_asle_3"
            END as "common_mental_health_symptoms_trouble_falling_or_staying_asle_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_feeling_tired_or_having_little__3' OR linkid = 'common_mental_health_symptoms_feeling_tired_or_having_little__3' OR short_name = 'common_mental_health_symptoms_feeling_tired_or_having_little__3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_feeling_tired_or_having_little__3"
            END as "common_mental_health_symptoms_feeling_tired_or_having_little__3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_poor_appetite_or_overeating_3' OR linkid = 'common_mental_health_symptoms_poor_appetite_or_overeating_3' OR short_name = 'common_mental_health_symptoms_poor_appetite_or_overeating_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_poor_appetite_or_overeating_3"
            END as "common_mental_health_symptoms_poor_appetite_or_overeating_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_feeling_bad_about_yourself_or_t_3' OR linkid = 'common_mental_health_symptoms_feeling_bad_about_yourself_or_t_3' OR short_name = 'common_mental_health_symptoms_feeling_bad_about_yourself_or_t_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_feeling_bad_about_yourself_or_t_3"
            END as "common_mental_health_symptoms_feeling_bad_about_yourself_or_t_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_trouble_concentrating_on_things_3' OR linkid = 'common_mental_health_symptoms_trouble_concentrating_on_things_3' OR short_name = 'common_mental_health_symptoms_trouble_concentrating_on_things_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_trouble_concentrating_on_things_3"
            END as "common_mental_health_symptoms_trouble_concentrating_on_things_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_36' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_36' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_36')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_36"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_36",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data

