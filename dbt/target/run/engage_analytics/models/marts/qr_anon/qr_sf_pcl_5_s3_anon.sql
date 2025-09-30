
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_sf_pcl_5_s3_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_sf_pcl_5_s3
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_sf_pcl_5_s3'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_sf_pcl_5_s3"
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
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__25' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__25' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__25')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__25"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__25",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__26' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__26' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__26')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__26"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__26",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__27' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__27' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__27')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__27"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__27",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__28' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__28' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__28')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__28"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__28",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__29' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__29' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__29')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__29"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__29",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'pcl_5_item_7' OR linkid = 'pcl_5_item_7' OR short_name = 'pcl_5_item_7')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "pcl_5_item_7"
            END as "pcl_5_item_7",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__30' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__30' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__30')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__30"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__30",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_56' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_56' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_56')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_56"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_56",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_57' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_57' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_57')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_57"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_57",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__31' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__31' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__31')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__31"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__31",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'pcl_5_item_8' OR linkid = 'pcl_5_item_8' OR short_name = 'pcl_5_item_8')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "pcl_5_item_8"
            END as "pcl_5_item_8",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_58' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_58' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_58')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_58"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_58",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__32' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__32' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__32')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__32"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__32",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_59' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_59' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_59')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_59"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_59",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'form_f1_question_26_3' OR linkid = 'form_f1_question_26_3' OR short_name = 'form_f1_question_26_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "form_f1_question_26_3"
            END as "form_f1_question_26_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'form_f1_question_27_3' OR linkid = 'form_f1_question_27_3' OR short_name = 'form_f1_question_27_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "form_f1_question_27_3"
            END as "form_f1_question_27_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'form_f1_question_28_3' OR linkid = 'form_f1_question_28_3' OR short_name = 'form_f1_question_28_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "form_f1_question_28_3"
            END as "form_f1_question_28_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'form_f1_question_29_3' OR linkid = 'form_f1_question_29_3' OR short_name = 'form_f1_question_29_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "form_f1_question_29_3"
            END as "form_f1_question_29_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'form_f1_question_30_3' OR linkid = 'form_f1_question_30_3' OR short_name = 'form_f1_question_30_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "form_f1_question_30_3"
            END as "form_f1_question_30_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'pcl_5_item_9' OR linkid = 'pcl_5_item_9' OR short_name = 'pcl_5_item_9')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "pcl_5_item_9"
            END as "pcl_5_item_9",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_60' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_60' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_60')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_60"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_60",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data


  );