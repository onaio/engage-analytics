
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_phq_9_s1_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_phq_9_s1
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_phq_9_s1'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_phq_9_s1"
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
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_26' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_26' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_26')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_26"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_26",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_27' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_27' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_27')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_27"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_27",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'phq_9_ipc_session_2_how_difficult_have_these_problems_made_it_2' OR linkid = 'phq_9_ipc_session_2_how_difficult_have_these_problems_made_it_2' OR short_name = 'phq_9_ipc_session_2_how_difficult_have_these_problems_made_it_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "phq_9_ipc_session_2_how_difficult_have_these_problems_made_it_2"
            END as "phq_9_ipc_session_2_how_difficult_have_these_problems_made_it_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_28' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_28' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_28')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_28"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_28",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_age_5' OR linkid = 'add_family_member_registration_calculated_age_5' OR short_name = 'add_family_member_registration_calculated_age_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_age_5"
            END as "add_family_member_registration_calculated_age_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_month_5' OR linkid = 'add_family_member_registration_calculated_month_5' OR short_name = 'add_family_member_registration_calculated_month_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_month_5"
            END as "add_family_member_registration_calculated_month_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_year_5' OR linkid = 'add_family_member_registration_calculated_year_5' OR short_name = 'add_family_member_registration_calculated_year_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_year_5"
            END as "add_family_member_registration_calculated_year_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_date_of_birth_5' OR linkid = 'add_family_member_registration_date_of_birth_5' OR short_name = 'add_family_member_registration_date_of_birth_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_date_of_birth_5"
            END as "add_family_member_registration_date_of_birth_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_29' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_29' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_29')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_29"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_29",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_moving_or_speaking_so_slowly_th_2' OR linkid = 'common_mental_health_symptoms_moving_or_speaking_so_slowly_th_2' OR short_name = 'common_mental_health_symptoms_moving_or_speaking_so_slowly_th_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_moving_or_speaking_so_slowly_th_2"
            END as "common_mental_health_symptoms_moving_or_speaking_so_slowly_th_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_thoughts_that_you_would_be_bett_2' OR linkid = 'common_mental_health_symptoms_thoughts_that_you_would_be_bett_2' OR short_name = 'common_mental_health_symptoms_thoughts_that_you_would_be_bett_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_thoughts_that_you_would_be_bett_2"
            END as "common_mental_health_symptoms_thoughts_that_you_would_be_bett_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_little_interest_or_pleasure_in__2' OR linkid = 'common_mental_health_symptoms_little_interest_or_pleasure_in__2' OR short_name = 'common_mental_health_symptoms_little_interest_or_pleasure_in__2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_little_interest_or_pleasure_in__2"
            END as "common_mental_health_symptoms_little_interest_or_pleasure_in__2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_feeling_down_depressed_or_hopel_2' OR linkid = 'common_mental_health_symptoms_feeling_down_depressed_or_hopel_2' OR short_name = 'common_mental_health_symptoms_feeling_down_depressed_or_hopel_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_feeling_down_depressed_or_hopel_2"
            END as "common_mental_health_symptoms_feeling_down_depressed_or_hopel_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_trouble_falling_or_staying_asle_2' OR linkid = 'common_mental_health_symptoms_trouble_falling_or_staying_asle_2' OR short_name = 'common_mental_health_symptoms_trouble_falling_or_staying_asle_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_trouble_falling_or_staying_asle_2"
            END as "common_mental_health_symptoms_trouble_falling_or_staying_asle_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_feeling_tired_or_having_little__2' OR linkid = 'common_mental_health_symptoms_feeling_tired_or_having_little__2' OR short_name = 'common_mental_health_symptoms_feeling_tired_or_having_little__2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_feeling_tired_or_having_little__2"
            END as "common_mental_health_symptoms_feeling_tired_or_having_little__2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_poor_appetite_or_overeating_2' OR linkid = 'common_mental_health_symptoms_poor_appetite_or_overeating_2' OR short_name = 'common_mental_health_symptoms_poor_appetite_or_overeating_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_poor_appetite_or_overeating_2"
            END as "common_mental_health_symptoms_poor_appetite_or_overeating_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_feeling_bad_about_yourself_or_t_2' OR linkid = 'common_mental_health_symptoms_feeling_bad_about_yourself_or_t_2' OR short_name = 'common_mental_health_symptoms_feeling_bad_about_yourself_or_t_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_feeling_bad_about_yourself_or_t_2"
            END as "common_mental_health_symptoms_feeling_bad_about_yourself_or_t_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_trouble_concentrating_on_things_2' OR linkid = 'common_mental_health_symptoms_trouble_concentrating_on_things_2' OR short_name = 'common_mental_health_symptoms_trouble_concentrating_on_things_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_trouble_concentrating_on_things_2"
            END as "common_mental_health_symptoms_trouble_concentrating_on_things_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_30' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_30' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_30')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_30"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_30",
        
    
        
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
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_31' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_31' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_31')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_31"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_31",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data


  );