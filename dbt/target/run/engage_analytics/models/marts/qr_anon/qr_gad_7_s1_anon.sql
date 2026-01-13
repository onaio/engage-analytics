
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_gad_7_s1_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_gad_7_s1
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_gad_7_s1'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_gad_7_s1"
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
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirpa_9' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirpa_9' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirpa_9')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirpa_9"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirpa_9",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_10' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_10' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_10')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_10"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_10",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_11' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_11' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_11')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_11"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_11",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_12' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_12' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_12')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_12"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_12",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_age_2' OR linkid = 'add_family_member_registration_calculated_age_2' OR short_name = 'add_family_member_registration_calculated_age_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_age_2"
            END as "add_family_member_registration_calculated_age_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_month_2' OR linkid = 'add_family_member_registration_calculated_month_2' OR short_name = 'add_family_member_registration_calculated_month_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_month_2"
            END as "add_family_member_registration_calculated_month_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_year_2' OR linkid = 'add_family_member_registration_calculated_year_2' OR short_name = 'add_family_member_registration_calculated_year_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_year_2"
            END as "add_family_member_registration_calculated_year_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_date_of_birth_2' OR linkid = 'add_family_member_registration_date_of_birth_2' OR short_name = 'add_family_member_registration_date_of_birth_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_date_of_birth_2"
            END as "add_family_member_registration_date_of_birth_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_13' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_13' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_13')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_13"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_13",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_2' OR linkid = 'common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_2' OR short_name = 'common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_2"
            END as "common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_not_being_able_to_stop_or_contr_2' OR linkid = 'common_mental_health_symptoms_not_being_able_to_stop_or_contr_2' OR short_name = 'common_mental_health_symptoms_not_being_able_to_stop_or_contr_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_not_being_able_to_stop_or_contr_2"
            END as "common_mental_health_symptoms_not_being_able_to_stop_or_contr_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_worrying_too_much_about_differe_2' OR linkid = 'common_mental_health_symptoms_worrying_too_much_about_differe_2' OR short_name = 'common_mental_health_symptoms_worrying_too_much_about_differe_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_worrying_too_much_about_differe_2"
            END as "common_mental_health_symptoms_worrying_too_much_about_differe_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_trouble_relaxing_2' OR linkid = 'common_mental_health_symptoms_trouble_relaxing_2' OR short_name = 'common_mental_health_symptoms_trouble_relaxing_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_trouble_relaxing_2"
            END as "common_mental_health_symptoms_trouble_relaxing_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_being_so_restless_that_it_s_har_2' OR linkid = 'common_mental_health_symptoms_being_so_restless_that_it_s_har_2' OR short_name = 'common_mental_health_symptoms_being_so_restless_that_it_s_har_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_being_so_restless_that_it_s_har_2"
            END as "common_mental_health_symptoms_being_so_restless_that_it_s_har_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_becoming_easily_annoyed_or_irri_2' OR linkid = 'common_mental_health_symptoms_becoming_easily_annoyed_or_irri_2' OR short_name = 'common_mental_health_symptoms_becoming_easily_annoyed_or_irri_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_becoming_easily_annoyed_or_irri_2"
            END as "common_mental_health_symptoms_becoming_easily_annoyed_or_irri_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_feeling_afraid_as_if_something__2' OR linkid = 'common_mental_health_symptoms_feeling_afraid_as_if_something__2' OR short_name = 'common_mental_health_symptoms_feeling_afraid_as_if_something__2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_feeling_afraid_as_if_something__2"
            END as "common_mental_health_symptoms_feeling_afraid_as_if_something__2",
        
    
        
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
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_14' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_14' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_14')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_14"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_14",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data


  );