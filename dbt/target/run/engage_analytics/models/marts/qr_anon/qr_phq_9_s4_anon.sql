
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_phq_9_s4_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_phq_9_s4
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_phq_9_s4'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_phq_9_s4"
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
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_40' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_40' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_40')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_40"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_40",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_41' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_41' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_41')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_41"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_41",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_42' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_42' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_42')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_42"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_42",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'phq_9_ipc_session_2_how_difficult_have_these_problems_made_it_3' OR linkid = 'phq_9_ipc_session_2_how_difficult_have_these_problems_made_it_3' OR short_name = 'phq_9_ipc_session_2_how_difficult_have_these_problems_made_it_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "phq_9_ipc_session_2_how_difficult_have_these_problems_made_it_3"
            END as "phq_9_ipc_session_2_how_difficult_have_these_problems_made_it_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_moving_or_speaking_so_slowly_th_5' OR linkid = 'common_mental_health_symptoms_moving_or_speaking_so_slowly_th_5' OR short_name = 'common_mental_health_symptoms_moving_or_speaking_so_slowly_th_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_moving_or_speaking_so_slowly_th_5"
            END as "common_mental_health_symptoms_moving_or_speaking_so_slowly_th_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_thoughts_that_you_would_be_bett_5' OR linkid = 'common_mental_health_symptoms_thoughts_that_you_would_be_bett_5' OR short_name = 'common_mental_health_symptoms_thoughts_that_you_would_be_bett_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_thoughts_that_you_would_be_bett_5"
            END as "common_mental_health_symptoms_thoughts_that_you_would_be_bett_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_little_interest_or_pleasure_in__5' OR linkid = 'common_mental_health_symptoms_little_interest_or_pleasure_in__5' OR short_name = 'common_mental_health_symptoms_little_interest_or_pleasure_in__5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_little_interest_or_pleasure_in__5"
            END as "common_mental_health_symptoms_little_interest_or_pleasure_in__5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_feeling_down_depressed_or_hopel_5' OR linkid = 'common_mental_health_symptoms_feeling_down_depressed_or_hopel_5' OR short_name = 'common_mental_health_symptoms_feeling_down_depressed_or_hopel_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_feeling_down_depressed_or_hopel_5"
            END as "common_mental_health_symptoms_feeling_down_depressed_or_hopel_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_trouble_falling_or_staying_asle_5' OR linkid = 'common_mental_health_symptoms_trouble_falling_or_staying_asle_5' OR short_name = 'common_mental_health_symptoms_trouble_falling_or_staying_asle_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_trouble_falling_or_staying_asle_5"
            END as "common_mental_health_symptoms_trouble_falling_or_staying_asle_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_feeling_tired_or_having_little__5' OR linkid = 'common_mental_health_symptoms_feeling_tired_or_having_little__5' OR short_name = 'common_mental_health_symptoms_feeling_tired_or_having_little__5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_feeling_tired_or_having_little__5"
            END as "common_mental_health_symptoms_feeling_tired_or_having_little__5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_poor_appetite_or_overeating_5' OR linkid = 'common_mental_health_symptoms_poor_appetite_or_overeating_5' OR short_name = 'common_mental_health_symptoms_poor_appetite_or_overeating_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_poor_appetite_or_overeating_5"
            END as "common_mental_health_symptoms_poor_appetite_or_overeating_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_feeling_bad_about_yourself_or_t_5' OR linkid = 'common_mental_health_symptoms_feeling_bad_about_yourself_or_t_5' OR short_name = 'common_mental_health_symptoms_feeling_bad_about_yourself_or_t_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_feeling_bad_about_yourself_or_t_5"
            END as "common_mental_health_symptoms_feeling_bad_about_yourself_or_t_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_trouble_concentrating_on_things_5' OR linkid = 'common_mental_health_symptoms_trouble_concentrating_on_things_5' OR short_name = 'common_mental_health_symptoms_trouble_concentrating_on_things_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_trouble_concentrating_on_things_5"
            END as "common_mental_health_symptoms_trouble_concentrating_on_things_5",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data


  );