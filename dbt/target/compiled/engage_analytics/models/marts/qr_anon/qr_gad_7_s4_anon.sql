

-- Anonymized view for qr_gad_7_s4
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_gad_7_s4'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_gad_7_s4"
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
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_23' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_23' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_23')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_23"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_23",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_24' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_24' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_24')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_24"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_24",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_25' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_25' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_25')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_25"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_25",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_5' OR linkid = 'common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_5' OR short_name = 'common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_5"
            END as "common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_not_being_able_to_stop_or_contr_5' OR linkid = 'common_mental_health_symptoms_not_being_able_to_stop_or_contr_5' OR short_name = 'common_mental_health_symptoms_not_being_able_to_stop_or_contr_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_not_being_able_to_stop_or_contr_5"
            END as "common_mental_health_symptoms_not_being_able_to_stop_or_contr_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_worrying_too_much_about_differe_5' OR linkid = 'common_mental_health_symptoms_worrying_too_much_about_differe_5' OR short_name = 'common_mental_health_symptoms_worrying_too_much_about_differe_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_worrying_too_much_about_differe_5"
            END as "common_mental_health_symptoms_worrying_too_much_about_differe_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_trouble_relaxing_5' OR linkid = 'common_mental_health_symptoms_trouble_relaxing_5' OR short_name = 'common_mental_health_symptoms_trouble_relaxing_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_trouble_relaxing_5"
            END as "common_mental_health_symptoms_trouble_relaxing_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_being_so_restless_that_it_s_har_5' OR linkid = 'common_mental_health_symptoms_being_so_restless_that_it_s_har_5' OR short_name = 'common_mental_health_symptoms_being_so_restless_that_it_s_har_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_being_so_restless_that_it_s_har_5"
            END as "common_mental_health_symptoms_being_so_restless_that_it_s_har_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_becoming_easily_annoyed_or_irri_5' OR linkid = 'common_mental_health_symptoms_becoming_easily_annoyed_or_irri_5' OR short_name = 'common_mental_health_symptoms_becoming_easily_annoyed_or_irri_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_becoming_easily_annoyed_or_irri_5"
            END as "common_mental_health_symptoms_becoming_easily_annoyed_or_irri_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_feeling_afraid_as_if_something__5' OR linkid = 'common_mental_health_symptoms_feeling_afraid_as_if_something__5' OR short_name = 'common_mental_health_symptoms_feeling_afraid_as_if_something__5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_feeling_afraid_as_if_something__5"
            END as "common_mental_health_symptoms_feeling_afraid_as_if_something__5",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data

