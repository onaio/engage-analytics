
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_start_ipc_s1_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_start_ipc_s1
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_start_ipc_s1'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_start_ipc_s1"
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
                    WHERE ("column" = 'person_2' OR linkid = 'person_2' OR short_name = 'person_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "person_2"
            END as "person_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_69' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_69' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_69')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_69"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_69",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_70' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_70' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_70')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_70"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_70",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_71' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_71' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_71')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_71"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_71",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_what_about_before_that_was_there_a_time_before_in' OR linkid = 'ipc_session_1_what_about_before_that_was_there_a_time_before_in' OR short_name = 'ipc_session_1_what_about_before_that_was_there_a_time_before_in')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_what_about_before_that_was_there_a_time_before_in"
            END as "ipc_session_1_what_about_before_that_was_there_a_time_before_in",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_72' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_72' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_72')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_72"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_72",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_73' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_73' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_73')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_73"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_73",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_earlier_than_that_you_noticed_symptoms' OR linkid = 'ipc_session_1_earlier_than_that_you_noticed_symptoms' OR short_name = 'ipc_session_1_earlier_than_that_you_noticed_symptoms')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_earlier_than_that_you_noticed_symptoms"
            END as "ipc_session_1_earlier_than_that_you_noticed_symptoms",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_is_there_anything_else_that_we_did_not_discuss_th' OR linkid = 'ipc_session_1_is_there_anything_else_that_we_did_not_discuss_th' OR short_name = 'ipc_session_1_is_there_anything_else_that_we_did_not_discuss_th')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_is_there_anything_else_that_we_did_not_discuss_th"
            END as "ipc_session_1_is_there_anything_else_that_we_did_not_discuss_th",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1' OR linkid = 'ipc_session_1' OR short_name = 'ipc_session_1')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1"
            END as "ipc_session_1",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_possible_problem_areas' OR linkid = 'ipc_session_1_possible_problem_areas' OR short_name = 'ipc_session_1_possible_problem_areas')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_possible_problem_areas"
            END as "ipc_session_1_possible_problem_areas",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_74' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_74' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_74')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_74"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_74",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_what_were_the_symptoms' OR linkid = 'ipc_session_1_what_were_the_symptoms' OR short_name = 'ipc_session_1_what_were_the_symptoms')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_what_were_the_symptoms"
            END as "ipc_session_1_what_were_the_symptoms",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_2' OR linkid = 'ipc_session_1_2' OR short_name = 'ipc_session_1_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_2"
            END as "ipc_session_1_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_3' OR linkid = 'ipc_session_1_3' OR short_name = 'ipc_session_1_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_3"
            END as "ipc_session_1_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_75' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_75' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_75')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_75"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_75",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'survey_response_9' OR linkid = 'survey_response_9' OR short_name = 'survey_response_9')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "survey_response_9"
            END as "survey_response_9",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_your_most_distressing_current_symptoms_began' OR linkid = 'ipc_session_1_your_most_distressing_current_symptoms_began' OR short_name = 'ipc_session_1_your_most_distressing_current_symptoms_began')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_your_most_distressing_current_symptoms_began"
            END as "ipc_session_1_your_most_distressing_current_symptoms_began",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_when_did_these_symptoms_begin' OR linkid = 'ipc_session_1_when_did_these_symptoms_begin' OR short_name = 'ipc_session_1_when_did_these_symptoms_begin')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_when_did_these_symptoms_begin"
            END as "ipc_session_1_when_did_these_symptoms_begin",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_when_did_you_notice_these_symptoms' OR linkid = 'ipc_session_1_when_did_you_notice_these_symptoms' OR short_name = 'ipc_session_1_when_did_you_notice_these_symptoms')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_when_did_you_notice_these_symptoms"
            END as "ipc_session_1_when_did_you_notice_these_symptoms",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_these_are_the_symptoms_you_said_you_are_currently' OR linkid = 'ipc_session_1_these_are_the_symptoms_you_said_you_are_currently' OR short_name = 'ipc_session_1_these_are_the_symptoms_you_said_you_are_currently')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_these_are_the_symptoms_you_said_you_are_currently"
            END as "ipc_session_1_these_are_the_symptoms_you_said_you_are_currently",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_76' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_76' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_76')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_76"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_76",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'person_3' OR linkid = 'person_3' OR short_name = 'person_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "person_3"
            END as "person_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_patient_age' OR linkid = 'ipc_session_1_patient_age' OR short_name = 'ipc_session_1_patient_age')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_patient_age"
            END as "ipc_session_1_patient_age",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_4' OR linkid = 'ipc_session_1_4' OR short_name = 'ipc_session_1_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_4"
            END as "ipc_session_1_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_i_noticed_that_when_i_was_asking_about_important_' OR linkid = 'ipc_session_1_i_noticed_that_when_i_was_asking_about_important_' OR short_name = 'ipc_session_1_i_noticed_that_when_i_was_asking_about_important_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_i_noticed_that_when_i_was_asking_about_important_"
            END as "ipc_session_1_i_noticed_that_when_i_was_asking_about_important_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_patient_dob' OR linkid = 'ipc_session_1_patient_dob' OR short_name = 'ipc_session_1_patient_dob')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_patient_dob"
            END as "ipc_session_1_patient_dob",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_5' OR linkid = 'ipc_session_1_5' OR short_name = 'ipc_session_1_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_5"
            END as "ipc_session_1_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_77' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_77' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_77')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_77"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_77",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mood_rating_ipc_session_1_2' OR linkid = 'mood_rating_ipc_session_1_2' OR short_name = 'mood_rating_ipc_session_1_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mood_rating_ipc_session_1_2"
            END as "mood_rating_ipc_session_1_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_6' OR linkid = 'ipc_session_1_6' OR short_name = 'ipc_session_1_6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_6"
            END as "ipc_session_1_6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_78' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_78' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_78')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_78"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_78",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_79' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_79' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_79')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_79"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_79",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_80' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_80' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_80')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_80"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_80",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_do_you_agree_with_this_summary' OR linkid = 'ipc_session_1_do_you_agree_with_this_summary' OR short_name = 'ipc_session_1_do_you_agree_with_this_summary')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_do_you_agree_with_this_summary"
            END as "ipc_session_1_do_you_agree_with_this_summary",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_in_what_format_did_you_deliver_this_session_with_' OR linkid = 'ipc_session_1_in_what_format_did_you_deliver_this_session_with_' OR short_name = 'ipc_session_1_in_what_format_did_you_deliver_this_session_with_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_in_what_format_did_you_deliver_this_session_with_"
            END as "ipc_session_1_in_what_format_did_you_deliver_this_session_with_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_7' OR linkid = 'ipc_session_1_7' OR short_name = 'ipc_session_1_7')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_7"
            END as "ipc_session_1_7",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_81' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_81' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_81')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_81"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_81",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_2_did_you_complete_the_required_assessment_s_and_th' OR linkid = 'ipc_session_2_did_you_complete_the_required_assessment_s_and_th' OR short_name = 'ipc_session_2_did_you_complete_the_required_assessment_s_and_th')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_2_did_you_complete_the_required_assessment_s_and_th"
            END as "ipc_session_2_did_you_complete_the_required_assessment_s_and_th",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_age_10' OR linkid = 'add_family_member_registration_calculated_age_10' OR short_name = 'add_family_member_registration_calculated_age_10')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_age_10"
            END as "add_family_member_registration_calculated_age_10",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_is_there_anything_that_could_get_in_the_way_of_yo' OR linkid = 'ipc_session_1_is_there_anything_that_could_get_in_the_way_of_yo' OR short_name = 'ipc_session_1_is_there_anything_that_could_get_in_the_way_of_yo')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_is_there_anything_that_could_get_in_the_way_of_yo"
            END as "ipc_session_1_is_there_anything_that_could_get_in_the_way_of_yo",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_82' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_82' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_82')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_82"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_82",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_what_was_going_on_in_your_life_when_you_started_t' OR linkid = 'ipc_session_1_what_was_going_on_in_your_life_when_you_started_t' OR short_name = 'ipc_session_1_what_was_going_on_in_your_life_when_you_started_t')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_what_was_going_on_in_your_life_when_you_started_t"
            END as "ipc_session_1_what_was_going_on_in_your_life_when_you_started_t",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_month_10' OR linkid = 'add_family_member_registration_calculated_month_10' OR short_name = 'add_family_member_registration_calculated_month_10')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_month_10"
            END as "add_family_member_registration_calculated_month_10",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_year_10' OR linkid = 'add_family_member_registration_calculated_year_10' OR short_name = 'add_family_member_registration_calculated_year_10')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_year_10"
            END as "add_family_member_registration_calculated_year_10",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_8' OR linkid = 'ipc_session_1_8' OR short_name = 'ipc_session_1_8')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_8"
            END as "ipc_session_1_8",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_date_of_birth_10' OR linkid = 'add_family_member_registration_date_of_birth_10' OR short_name = 'add_family_member_registration_date_of_birth_10')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_date_of_birth_10"
            END as "add_family_member_registration_date_of_birth_10",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_83' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_83' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_83')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_83"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_83",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mood_rating_ipc_session_1_total_score_2' OR linkid = 'mood_rating_ipc_session_1_total_score_2' OR short_name = 'mood_rating_ipc_session_1_total_score_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mood_rating_ipc_session_1_total_score_2"
            END as "mood_rating_ipc_session_1_total_score_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_84' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_84' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_84')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_84"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_84",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_what_was_happening_then' OR linkid = 'ipc_session_1_what_was_happening_then' OR short_name = 'ipc_session_1_what_was_happening_then')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_what_was_happening_then"
            END as "ipc_session_1_what_was_happening_then",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_9' OR linkid = 'ipc_session_1_9' OR short_name = 'ipc_session_1_9')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_9"
            END as "ipc_session_1_9",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_85' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_85' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_85')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_85"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_85",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_of_these_symptoms_which_are_the_most_distressing' OR linkid = 'ipc_session_1_of_these_symptoms_which_are_the_most_distressing' OR short_name = 'ipc_session_1_of_these_symptoms_which_are_the_most_distressing')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_of_these_symptoms_which_are_the_most_distressing"
            END as "ipc_session_1_of_these_symptoms_which_are_the_most_distressing",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_86' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_86' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_86')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_86"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_86",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_at_that_time_you_were_experiencing' OR linkid = 'ipc_session_1_at_that_time_you_were_experiencing' OR short_name = 'ipc_session_1_at_that_time_you_were_experiencing')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_at_that_time_you_were_experiencing"
            END as "ipc_session_1_at_that_time_you_were_experiencing",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being__5' OR linkid = 'mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being__5' OR short_name = 'mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being__5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being__5"
            END as "mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being__5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'form_f1_question_38' OR linkid = 'form_f1_question_38' OR short_name = 'form_f1_question_38')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "form_f1_question_38"
            END as "form_f1_question_38",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_10' OR linkid = 'ipc_session_1_10' OR short_name = 'ipc_session_1_10')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_10"
            END as "ipc_session_1_10",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'person_1' OR linkid = 'person_1' OR short_name = 'person_1')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "person_1"
            END as "person_1",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'person_1_2' OR linkid = 'person_1_2' OR short_name = 'person_1_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "person_1_2"
            END as "person_1_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'person_1_how_has_this_relationship_changed_following_trauma_if_' OR linkid = 'person_1_how_has_this_relationship_changed_following_trauma_if_' OR short_name = 'person_1_how_has_this_relationship_changed_following_trauma_if_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "person_1_how_has_this_relationship_changed_following_trauma_if_"
            END as "person_1_how_has_this_relationship_changed_following_trauma_if_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'person_1_person_1_is' OR linkid = 'person_1_person_1_is' OR short_name = 'person_1_person_1_is')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "person_1_person_1_is"
            END as "person_1_person_1_is",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'person_2_2' OR linkid = 'person_2_2' OR short_name = 'person_2_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "person_2_2"
            END as "person_2_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'person_2_3' OR linkid = 'person_2_3' OR short_name = 'person_2_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "person_2_3"
            END as "person_2_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'person_2_how_has_this_relationship_changed_following_trauma_if_' OR linkid = 'person_2_how_has_this_relationship_changed_following_trauma_if_' OR short_name = 'person_2_how_has_this_relationship_changed_following_trauma_if_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "person_2_how_has_this_relationship_changed_following_trauma_if_"
            END as "person_2_how_has_this_relationship_changed_following_trauma_if_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'person_2_person_2_is' OR linkid = 'person_2_person_2_is' OR short_name = 'person_2_person_2_is')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "person_2_person_2_is"
            END as "person_2_person_2_is",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'person_3_2' OR linkid = 'person_3_2' OR short_name = 'person_3_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "person_3_2"
            END as "person_3_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'person_3_3' OR linkid = 'person_3_3' OR short_name = 'person_3_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "person_3_3"
            END as "person_3_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'person_3_how_has_this_relationship_changed_following_trauma_if_' OR linkid = 'person_3_how_has_this_relationship_changed_following_trauma_if_' OR short_name = 'person_3_how_has_this_relationship_changed_following_trauma_if_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "person_3_how_has_this_relationship_changed_following_trauma_if_"
            END as "person_3_how_has_this_relationship_changed_following_trauma_if_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'person_3_person_3_is' OR linkid = 'person_3_person_3_is' OR short_name = 'person_3_person_3_is')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "person_3_person_3_is"
            END as "person_3_person_3_is",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'person_4' OR linkid = 'person_4' OR short_name = 'person_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "person_4"
            END as "person_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'person_4_2' OR linkid = 'person_4_2' OR short_name = 'person_4_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "person_4_2"
            END as "person_4_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'person_4_how_has_this_relationship_changed_following_trauma_if_' OR linkid = 'person_4_how_has_this_relationship_changed_following_trauma_if_' OR short_name = 'person_4_how_has_this_relationship_changed_following_trauma_if_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "person_4_how_has_this_relationship_changed_following_trauma_if_"
            END as "person_4_how_has_this_relationship_changed_following_trauma_if_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'person_4_person_4_is' OR linkid = 'person_4_person_4_is' OR short_name = 'person_4_person_4_is')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "person_4_person_4_is"
            END as "person_4_person_4_is",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_11' OR linkid = 'ipc_session_1_11' OR short_name = 'ipc_session_1_11')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_11"
            END as "ipc_session_1_11",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_has_this_person_been_consistently_close_to_you_or' OR linkid = 'ipc_session_1_has_this_person_been_consistently_close_to_you_or' OR short_name = 'ipc_session_1_has_this_person_been_consistently_close_to_you_or')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_has_this_person_been_consistently_close_to_you_or"
            END as "ipc_session_1_has_this_person_been_consistently_close_to_you_or",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_12' OR linkid = 'ipc_session_1_12' OR short_name = 'ipc_session_1_12')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_12"
            END as "ipc_session_1_12",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_has_this_person_been_consistently_close_to_you__2' OR linkid = 'ipc_session_1_has_this_person_been_consistently_close_to_you__2' OR short_name = 'ipc_session_1_has_this_person_been_consistently_close_to_you__2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_has_this_person_been_consistently_close_to_you__2"
            END as "ipc_session_1_has_this_person_been_consistently_close_to_you__2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_13' OR linkid = 'ipc_session_1_13' OR short_name = 'ipc_session_1_13')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_13"
            END as "ipc_session_1_13",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_has_this_person_been_consistently_close_to_you__3' OR linkid = 'ipc_session_1_has_this_person_been_consistently_close_to_you__3' OR short_name = 'ipc_session_1_has_this_person_been_consistently_close_to_you__3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_has_this_person_been_consistently_close_to_you__3"
            END as "ipc_session_1_has_this_person_been_consistently_close_to_you__3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_14' OR linkid = 'ipc_session_1_14' OR short_name = 'ipc_session_1_14')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_14"
            END as "ipc_session_1_14",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_has_this_person_been_consistently_close_to_you__4' OR linkid = 'ipc_session_1_has_this_person_been_consistently_close_to_you__4' OR short_name = 'ipc_session_1_has_this_person_been_consistently_close_to_you__4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_has_this_person_been_consistently_close_to_you__4"
            END as "ipc_session_1_has_this_person_been_consistently_close_to_you__4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_do_you_think_an_important_person_s_death_is_conne' OR linkid = 'ipc_session_1_do_you_think_an_important_person_s_death_is_conne' OR short_name = 'ipc_session_1_do_you_think_an_important_person_s_death_is_conne')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_do_you_think_an_important_person_s_death_is_conne"
            END as "ipc_session_1_do_you_think_an_important_person_s_death_is_conne",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_tell_me_about_your_relationship_with_this_person_' OR linkid = 'ipc_session_1_tell_me_about_your_relationship_with_this_person_' OR short_name = 'ipc_session_1_tell_me_about_your_relationship_with_this_person_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_tell_me_about_your_relationship_with_this_person_"
            END as "ipc_session_1_tell_me_about_your_relationship_with_this_person_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'form_f1_question_46' OR linkid = 'form_f1_question_46' OR short_name = 'form_f1_question_46')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "form_f1_question_46"
            END as "form_f1_question_46",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_are_you_and_someone_else_having_a_strong_disagree' OR linkid = 'ipc_session_1_are_you_and_someone_else_having_a_strong_disagree' OR short_name = 'ipc_session_1_are_you_and_someone_else_having_a_strong_disagree')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_are_you_and_someone_else_having_a_strong_disagree"
            END as "ipc_session_1_are_you_and_someone_else_having_a_strong_disagree",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_briefly_tell_me_about_the_disagreement' OR linkid = 'ipc_session_1_briefly_tell_me_about_the_disagreement' OR short_name = 'ipc_session_1_briefly_tell_me_about_the_disagreement')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_briefly_tell_me_about_the_disagreement"
            END as "ipc_session_1_briefly_tell_me_about_the_disagreement",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_when_did_this_disagreement_happen' OR linkid = 'ipc_session_1_when_did_this_disagreement_happen' OR short_name = 'ipc_session_1_when_did_this_disagreement_happen')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_when_did_this_disagreement_happen"
            END as "ipc_session_1_when_did_this_disagreement_happen",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_how_has_this_disagreement_been_affecting_you' OR linkid = 'ipc_session_1_how_has_this_disagreement_been_affecting_you' OR short_name = 'ipc_session_1_how_has_this_disagreement_been_affecting_you')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_how_has_this_disagreement_been_affecting_you"
            END as "ipc_session_1_how_has_this_disagreement_been_affecting_you",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_have_there_been_any_changes_in_your_life_that_are' OR linkid = 'ipc_session_1_have_there_been_any_changes_in_your_life_that_are' OR short_name = 'ipc_session_1_have_there_been_any_changes_in_your_life_that_are')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_have_there_been_any_changes_in_your_life_that_are"
            END as "ipc_session_1_have_there_been_any_changes_in_your_life_that_are",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_what_was_the_change' OR linkid = 'ipc_session_1_what_was_the_change' OR short_name = 'ipc_session_1_what_was_the_change')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_what_was_the_change"
            END as "ipc_session_1_what_was_the_change",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_when_did_this_happen' OR linkid = 'ipc_session_1_when_did_this_happen' OR short_name = 'ipc_session_1_when_did_this_happen')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_when_did_this_happen"
            END as "ipc_session_1_when_did_this_happen",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_if_the_client_did_not_mention_any_close_relations' OR linkid = 'ipc_session_1_if_the_client_did_not_mention_any_close_relations' OR short_name = 'ipc_session_1_if_the_client_did_not_mention_any_close_relations')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_if_the_client_did_not_mention_any_close_relations"
            END as "ipc_session_1_if_the_client_did_not_mention_any_close_relations",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_would_you_say_you_have_close_relationships' OR linkid = 'ipc_session_1_would_you_say_you_have_close_relationships' OR short_name = 'ipc_session_1_would_you_say_you_have_close_relationships')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_would_you_say_you_have_close_relationships"
            END as "ipc_session_1_would_you_say_you_have_close_relationships",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_has_anything_changed_in_your_life_so_that_now_you' OR linkid = 'ipc_session_1_has_anything_changed_in_your_life_so_that_now_you' OR short_name = 'ipc_session_1_has_anything_changed_in_your_life_so_that_now_you')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_has_anything_changed_in_your_life_so_that_now_you"
            END as "ipc_session_1_has_anything_changed_in_your_life_so_that_now_you",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_when_did_this_event_happen' OR linkid = 'ipc_session_1_when_did_this_event_happen' OR short_name = 'ipc_session_1_when_did_this_event_happen')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_when_did_this_event_happen"
            END as "ipc_session_1_when_did_this_event_happen",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_15' OR linkid = 'ipc_session_1_15' OR short_name = 'ipc_session_1_15')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_15"
            END as "ipc_session_1_15",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_16' OR linkid = 'ipc_session_1_16' OR short_name = 'ipc_session_1_16')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_16"
            END as "ipc_session_1_16",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_do_you_want_to_propose_an_interpersonal_formulati' OR linkid = 'ipc_session_1_do_you_want_to_propose_an_interpersonal_formulati' OR short_name = 'ipc_session_1_do_you_want_to_propose_an_interpersonal_formulati')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_do_you_want_to_propose_an_interpersonal_formulati"
            END as "ipc_session_1_do_you_want_to_propose_an_interpersonal_formulati",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_do_you_agree_with_this' OR linkid = 'ipc_session_1_do_you_agree_with_this' OR short_name = 'ipc_session_1_do_you_agree_with_this')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_do_you_agree_with_this"
            END as "ipc_session_1_do_you_agree_with_this",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_17' OR linkid = 'ipc_session_1_17' OR short_name = 'ipc_session_1_17')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_17"
            END as "ipc_session_1_17",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_select_the_possible_interpersonal_problem_area_s_' OR linkid = 'ipc_session_1_select_the_possible_interpersonal_problem_area_s_' OR short_name = 'ipc_session_1_select_the_possible_interpersonal_problem_area_s_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_select_the_possible_interpersonal_problem_area_s_"
            END as "ipc_session_1_select_the_possible_interpersonal_problem_area_s_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_18' OR linkid = 'ipc_session_1_18' OR short_name = 'ipc_session_1_18')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_18"
            END as "ipc_session_1_18",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'ipc_session_1_at_that_time_you_were_experiencing_2' OR linkid = 'ipc_session_1_at_that_time_you_were_experiencing_2' OR short_name = 'ipc_session_1_at_that_time_you_were_experiencing_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "ipc_session_1_at_that_time_you_were_experiencing_2"
            END as "ipc_session_1_at_that_time_you_were_experiencing_2",
        
    
        
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
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_87' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_87' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_87')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_87"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_87",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_88' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_88' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_88')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_88"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_88",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_89' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_89' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_89')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_89"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_89",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_90' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_90' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_90')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_90"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_90",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_91' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_91' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_91')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_91"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_91",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data


  );