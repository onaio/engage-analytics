

-- Anonymized view for qr_sf_pcl_5_s2
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_sf_pcl_5_s2'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_sf_pcl_5_s2"
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
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__17' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__17' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__17')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__17"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__17",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__18' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__18' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__18')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__18"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__18",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__19' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__19' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__19')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__19"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__19",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__20' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__20' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__20')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__20"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__20",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__21' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__21' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__21')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__21"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__21",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'pcl_5_item_4' OR linkid = 'pcl_5_item_4' OR short_name = 'pcl_5_item_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "pcl_5_item_4"
            END as "pcl_5_item_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_50' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_50' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_50')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_50"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_50",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__22' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__22' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__22')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__22"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__22",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'pcl_5_item_5' OR linkid = 'pcl_5_item_5' OR short_name = 'pcl_5_item_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "pcl_5_item_5"
            END as "pcl_5_item_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_51' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_51' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_51')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_51"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_51",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'pcl_5_item_6' OR linkid = 'pcl_5_item_6' OR short_name = 'pcl_5_item_6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "pcl_5_item_6"
            END as "pcl_5_item_6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__23' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__23' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__23')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__23"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__23",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_52' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_52' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_52')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_52"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_52",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__24' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__24' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__24')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__24"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__24",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_53' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_53' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_53')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_53"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_53",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'form_f1_question_26_2' OR linkid = 'form_f1_question_26_2' OR short_name = 'form_f1_question_26_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "form_f1_question_26_2"
            END as "form_f1_question_26_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'form_f1_question_27_2' OR linkid = 'form_f1_question_27_2' OR short_name = 'form_f1_question_27_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "form_f1_question_27_2"
            END as "form_f1_question_27_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'form_f1_question_28_2' OR linkid = 'form_f1_question_28_2' OR short_name = 'form_f1_question_28_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "form_f1_question_28_2"
            END as "form_f1_question_28_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'form_f1_question_29_2' OR linkid = 'form_f1_question_29_2' OR short_name = 'form_f1_question_29_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "form_f1_question_29_2"
            END as "form_f1_question_29_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'form_f1_question_30_2' OR linkid = 'form_f1_question_30_2' OR short_name = 'form_f1_question_30_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "form_f1_question_30_2"
            END as "form_f1_question_30_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_54' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_54' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_54')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_54"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_54",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_55' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_55' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_55')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_55"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_55",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data

