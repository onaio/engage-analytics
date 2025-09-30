

-- Anonymized view for qr_gad_7_s2
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_gad_7_s2'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_gad_7_s2"
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
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_15' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_15' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_15')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_15"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_15",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_16' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_16' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_16')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_16"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_16",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_17' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_17' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_17')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_17"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_17",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_18' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_18' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_18')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_18"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_18",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_3' OR linkid = 'common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_3' OR short_name = 'common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_3"
            END as "common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_not_being_able_to_stop_or_contr_3' OR linkid = 'common_mental_health_symptoms_not_being_able_to_stop_or_contr_3' OR short_name = 'common_mental_health_symptoms_not_being_able_to_stop_or_contr_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_not_being_able_to_stop_or_contr_3"
            END as "common_mental_health_symptoms_not_being_able_to_stop_or_contr_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_worrying_too_much_about_differe_3' OR linkid = 'common_mental_health_symptoms_worrying_too_much_about_differe_3' OR short_name = 'common_mental_health_symptoms_worrying_too_much_about_differe_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_worrying_too_much_about_differe_3"
            END as "common_mental_health_symptoms_worrying_too_much_about_differe_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_trouble_relaxing_3' OR linkid = 'common_mental_health_symptoms_trouble_relaxing_3' OR short_name = 'common_mental_health_symptoms_trouble_relaxing_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_trouble_relaxing_3"
            END as "common_mental_health_symptoms_trouble_relaxing_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_being_so_restless_that_it_s_har_3' OR linkid = 'common_mental_health_symptoms_being_so_restless_that_it_s_har_3' OR short_name = 'common_mental_health_symptoms_being_so_restless_that_it_s_har_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_being_so_restless_that_it_s_har_3"
            END as "common_mental_health_symptoms_being_so_restless_that_it_s_har_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_becoming_easily_annoyed_or_irri_3' OR linkid = 'common_mental_health_symptoms_becoming_easily_annoyed_or_irri_3' OR short_name = 'common_mental_health_symptoms_becoming_easily_annoyed_or_irri_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_becoming_easily_annoyed_or_irri_3"
            END as "common_mental_health_symptoms_becoming_easily_annoyed_or_irri_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_feeling_afraid_as_if_something__3' OR linkid = 'common_mental_health_symptoms_feeling_afraid_as_if_something__3' OR short_name = 'common_mental_health_symptoms_feeling_afraid_as_if_something__3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_feeling_afraid_as_if_something__3"
            END as "common_mental_health_symptoms_feeling_afraid_as_if_something__3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_19' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_19' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_19')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_19"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_19",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data

