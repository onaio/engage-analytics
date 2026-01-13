

-- Anonymized view for qr_sf_pcl_5_s1
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_sf_pcl_5_s1'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_sf_pcl_5_s1"
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
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_43' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_43' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_43')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_43"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_43",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_44' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_44' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_44')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_44"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_44",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_9' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_9' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_9')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_9"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_9",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__10' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__10' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__10')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__10"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__10",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__11' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__11' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__11')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__11"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__11",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__12' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__12' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__12')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__12"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__12",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__13' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__13' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__13')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__13"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__13",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'pcl_5_item' OR linkid = 'pcl_5_item' OR short_name = 'pcl_5_item')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "pcl_5_item"
            END as "pcl_5_item",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__14' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__14' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__14')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__14"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__14",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_age_8' OR linkid = 'add_family_member_registration_calculated_age_8' OR short_name = 'add_family_member_registration_calculated_age_8')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_age_8"
            END as "add_family_member_registration_calculated_age_8",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'pcl_5_item_2' OR linkid = 'pcl_5_item_2' OR short_name = 'pcl_5_item_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "pcl_5_item_2"
            END as "pcl_5_item_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_month_8' OR linkid = 'add_family_member_registration_calculated_month_8' OR short_name = 'add_family_member_registration_calculated_month_8')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_month_8"
            END as "add_family_member_registration_calculated_month_8",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_year_8' OR linkid = 'add_family_member_registration_calculated_year_8' OR short_name = 'add_family_member_registration_calculated_year_8')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_year_8"
            END as "add_family_member_registration_calculated_year_8",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_date_of_birth_8' OR linkid = 'add_family_member_registration_date_of_birth_8' OR short_name = 'add_family_member_registration_date_of_birth_8')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_date_of_birth_8"
            END as "add_family_member_registration_date_of_birth_8",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_45' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_45' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_45')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_45"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_45",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'pcl_5_item_3' OR linkid = 'pcl_5_item_3' OR short_name = 'pcl_5_item_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "pcl_5_item_3"
            END as "pcl_5_item_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__15' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__15' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__15')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__15"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__15",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_46' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_46' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_46')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_46"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_46",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__16' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__16' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__16')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__16"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__16",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_47' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_47' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_47')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_47"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_47",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'form_f1_question_26' OR linkid = 'form_f1_question_26' OR short_name = 'form_f1_question_26')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "form_f1_question_26"
            END as "form_f1_question_26",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'form_f1_question_27' OR linkid = 'form_f1_question_27' OR short_name = 'form_f1_question_27')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "form_f1_question_27"
            END as "form_f1_question_27",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'form_f1_question_28' OR linkid = 'form_f1_question_28' OR short_name = 'form_f1_question_28')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "form_f1_question_28"
            END as "form_f1_question_28",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'form_f1_question_29' OR linkid = 'form_f1_question_29' OR short_name = 'form_f1_question_29')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "form_f1_question_29"
            END as "form_f1_question_29",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'form_f1_question_30' OR linkid = 'form_f1_question_30' OR short_name = 'form_f1_question_30')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "form_f1_question_30"
            END as "form_f1_question_30",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_48' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_48' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_48')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_48"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_48",
        
    
        
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
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_49' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_49' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_49')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_49"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_49",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data

