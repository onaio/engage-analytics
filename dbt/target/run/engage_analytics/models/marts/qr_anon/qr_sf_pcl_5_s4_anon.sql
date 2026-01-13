
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_sf_pcl_5_s4_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_sf_pcl_5_s4
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_sf_pcl_5_s4'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_sf_pcl_5_s4"
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
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__33' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__33' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__33')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__33"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__33",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__34' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__34' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__34')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__34"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__34",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__35' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__35' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__35')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__35"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__35",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_61' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_61' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_61')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_61"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_61",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__36' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__36' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__36')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__36"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__36",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__37' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__37' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__37')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__37"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__37",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'pcl_5_item_10' OR linkid = 'pcl_5_item_10' OR short_name = 'pcl_5_item_10')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "pcl_5_item_10"
            END as "pcl_5_item_10",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__38' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__38' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__38')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__38"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__38",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_62' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_62' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_62')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_62"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_62",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'pcl_5_item_11' OR linkid = 'pcl_5_item_11' OR short_name = 'pcl_5_item_11')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "pcl_5_item_11"
            END as "pcl_5_item_11",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'pcl_5_item_12' OR linkid = 'pcl_5_item_12' OR short_name = 'pcl_5_item_12')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "pcl_5_item_12"
            END as "pcl_5_item_12",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__39' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__39' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__39')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__39"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__39",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_63' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_63' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_63')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_63"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_63",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__40' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__40' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__40')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__40"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__40",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_64' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_64' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_64')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_64"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_64",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'form_f1_question_26_4' OR linkid = 'form_f1_question_26_4' OR short_name = 'form_f1_question_26_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "form_f1_question_26_4"
            END as "form_f1_question_26_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'form_f1_question_27_4' OR linkid = 'form_f1_question_27_4' OR short_name = 'form_f1_question_27_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "form_f1_question_27_4"
            END as "form_f1_question_27_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'form_f1_question_28_4' OR linkid = 'form_f1_question_28_4' OR short_name = 'form_f1_question_28_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "form_f1_question_28_4"
            END as "form_f1_question_28_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'form_f1_question_29_4' OR linkid = 'form_f1_question_29_4' OR short_name = 'form_f1_question_29_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "form_f1_question_29_4"
            END as "form_f1_question_29_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'form_f1_question_30_4' OR linkid = 'form_f1_question_30_4' OR short_name = 'form_f1_question_30_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "form_f1_question_30_4"
            END as "form_f1_question_30_4",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data


  );