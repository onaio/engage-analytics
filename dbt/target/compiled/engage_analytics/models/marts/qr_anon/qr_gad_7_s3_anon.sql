

-- Anonymized view for qr_gad_7_s3
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_gad_7_s3'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_gad_7_s3"
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
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_20' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_20' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_20')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_20"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_20",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_21' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_21' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_21')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_21"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_21",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_22' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_22' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_22')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_22"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_22",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_4' OR linkid = 'common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_4' OR short_name = 'common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_4"
            END as "common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_not_being_able_to_stop_or_contr_4' OR linkid = 'common_mental_health_symptoms_not_being_able_to_stop_or_contr_4' OR short_name = 'common_mental_health_symptoms_not_being_able_to_stop_or_contr_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_not_being_able_to_stop_or_contr_4"
            END as "common_mental_health_symptoms_not_being_able_to_stop_or_contr_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_worrying_too_much_about_differe_4' OR linkid = 'common_mental_health_symptoms_worrying_too_much_about_differe_4' OR short_name = 'common_mental_health_symptoms_worrying_too_much_about_differe_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_worrying_too_much_about_differe_4"
            END as "common_mental_health_symptoms_worrying_too_much_about_differe_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_trouble_relaxing_4' OR linkid = 'common_mental_health_symptoms_trouble_relaxing_4' OR short_name = 'common_mental_health_symptoms_trouble_relaxing_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_trouble_relaxing_4"
            END as "common_mental_health_symptoms_trouble_relaxing_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_being_so_restless_that_it_s_har_4' OR linkid = 'common_mental_health_symptoms_being_so_restless_that_it_s_har_4' OR short_name = 'common_mental_health_symptoms_being_so_restless_that_it_s_har_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_being_so_restless_that_it_s_har_4"
            END as "common_mental_health_symptoms_being_so_restless_that_it_s_har_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_becoming_easily_annoyed_or_irri_4' OR linkid = 'common_mental_health_symptoms_becoming_easily_annoyed_or_irri_4' OR short_name = 'common_mental_health_symptoms_becoming_easily_annoyed_or_irri_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_becoming_easily_annoyed_or_irri_4"
            END as "common_mental_health_symptoms_becoming_easily_annoyed_or_irri_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_feeling_afraid_as_if_something__4' OR linkid = 'common_mental_health_symptoms_feeling_afraid_as_if_something__4' OR short_name = 'common_mental_health_symptoms_feeling_afraid_as_if_something__4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_feeling_afraid_as_if_something__4"
            END as "common_mental_health_symptoms_feeling_afraid_as_if_something__4",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data

