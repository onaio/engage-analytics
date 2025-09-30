
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_spi_subform_1_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_spi_subform_1
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_spi_subform_1'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_spi_subform_1"
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
                    WHERE ("column" = 'spi_subform_1_is_medium_risk' OR linkid = 'spi_subform_1_is_medium_risk' OR short_name = 'spi_subform_1_is_medium_risk')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_1_is_medium_risk"
            END as "spi_subform_1_is_medium_risk",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_1_were_any_of_these_in_the_past_three_months' OR linkid = 'spi_subform_1_were_any_of_these_in_the_past_three_months' OR short_name = 'spi_subform_1_were_any_of_these_in_the_past_three_months')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_1_were_any_of_these_in_the_past_three_months"
            END as "spi_subform_1_were_any_of_these_in_the_past_three_months",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_1_in_the_past_month_have_you_been_thinking_about_ho' OR linkid = 'spi_subform_1_in_the_past_month_have_you_been_thinking_about_ho' OR short_name = 'spi_subform_1_in_the_past_month_have_you_been_thinking_about_ho')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_1_in_the_past_month_have_you_been_thinking_about_ho"
            END as "spi_subform_1_in_the_past_month_have_you_been_thinking_about_ho",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_1_is_high_risk' OR linkid = 'spi_subform_1_is_high_risk' OR short_name = 'spi_subform_1_is_high_risk')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_1_is_high_risk"
            END as "spi_subform_1_is_high_risk",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_1_in_the_past_month_have_you_had_these_thoughts_and' OR linkid = 'spi_subform_1_in_the_past_month_have_you_had_these_thoughts_and' OR short_name = 'spi_subform_1_in_the_past_month_have_you_had_these_thoughts_and')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_1_in_the_past_month_have_you_had_these_thoughts_and"
            END as "spi_subform_1_in_the_past_month_have_you_had_these_thoughts_and",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_1' OR linkid = 'spi_subform_1' OR short_name = 'spi_subform_1')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_1"
            END as "spi_subform_1",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_1_in_the_past_month_have_you_started_to_work_out_or' OR linkid = 'spi_subform_1_in_the_past_month_have_you_started_to_work_out_or' OR short_name = 'spi_subform_1_in_the_past_month_have_you_started_to_work_out_or')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_1_in_the_past_month_have_you_started_to_work_out_or"
            END as "spi_subform_1_in_the_past_month_have_you_started_to_work_out_or",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_1_2' OR linkid = 'spi_subform_1_2' OR short_name = 'spi_subform_1_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_1_2"
            END as "spi_subform_1_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_1_3' OR linkid = 'spi_subform_1_3' OR short_name = 'spi_subform_1_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_1_3"
            END as "spi_subform_1_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_1_in_the_past_month_have_you_wished_you_were_dead_o' OR linkid = 'spi_subform_1_in_the_past_month_have_you_wished_you_were_dead_o' OR short_name = 'spi_subform_1_in_the_past_month_have_you_wished_you_were_dead_o')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_1_in_the_past_month_have_you_wished_you_were_dead_o"
            END as "spi_subform_1_in_the_past_month_have_you_wished_you_were_dead_o",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_1_4' OR linkid = 'spi_subform_1_4' OR short_name = 'spi_subform_1_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_1_4"
            END as "spi_subform_1_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_1_have_you_ever_done_anything_started_to_do_anythin' OR linkid = 'spi_subform_1_have_you_ever_done_anything_started_to_do_anythin' OR short_name = 'spi_subform_1_have_you_ever_done_anything_started_to_do_anythin')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_1_have_you_ever_done_anything_started_to_do_anythin"
            END as "spi_subform_1_have_you_ever_done_anything_started_to_do_anythin",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_1_5' OR linkid = 'spi_subform_1_5' OR short_name = 'spi_subform_1_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_1_5"
            END as "spi_subform_1_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_1_6' OR linkid = 'spi_subform_1_6' OR short_name = 'spi_subform_1_6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_1_6"
            END as "spi_subform_1_6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_1_is_low_risk' OR linkid = 'spi_subform_1_is_low_risk' OR short_name = 'spi_subform_1_is_low_risk')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_1_is_low_risk"
            END as "spi_subform_1_is_low_risk",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_1_in_the_past_month_have_you_actually_had_any_thoug' OR linkid = 'spi_subform_1_in_the_past_month_have_you_actually_had_any_thoug' OR short_name = 'spi_subform_1_in_the_past_month_have_you_actually_had_any_thoug')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_1_in_the_past_month_have_you_actually_had_any_thoug"
            END as "spi_subform_1_in_the_past_month_have_you_actually_had_any_thoug",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_1_7' OR linkid = 'spi_subform_1_7' OR short_name = 'spi_subform_1_7')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_1_7"
            END as "spi_subform_1_7",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_65' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_65' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_65')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_65"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_65",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_1_taskid_spi_subform_2' OR linkid = 'spi_subform_1_taskid_spi_subform_2' OR short_name = 'spi_subform_1_taskid_spi_subform_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_1_taskid_spi_subform_2"
            END as "spi_subform_1_taskid_spi_subform_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_2_taskid_spi_subform_3' OR linkid = 'spi_subform_2_taskid_spi_subform_3' OR short_name = 'spi_subform_2_taskid_spi_subform_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_2_taskid_spi_subform_3"
            END as "spi_subform_2_taskid_spi_subform_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_2_taskid_spi_subform_4' OR linkid = 'spi_subform_2_taskid_spi_subform_4' OR short_name = 'spi_subform_2_taskid_spi_subform_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_2_taskid_spi_subform_4"
            END as "spi_subform_2_taskid_spi_subform_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_1_taskstatus_spi_subform_2' OR linkid = 'spi_subform_1_taskstatus_spi_subform_2' OR short_name = 'spi_subform_1_taskstatus_spi_subform_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_1_taskstatus_spi_subform_2"
            END as "spi_subform_1_taskstatus_spi_subform_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_2_taskstatus_spi_subform_3' OR linkid = 'spi_subform_2_taskstatus_spi_subform_3' OR short_name = 'spi_subform_2_taskstatus_spi_subform_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_2_taskstatus_spi_subform_3"
            END as "spi_subform_2_taskstatus_spi_subform_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'spi_subform_2_taskstatus_spi_subform_4' OR linkid = 'spi_subform_2_taskstatus_spi_subform_4' OR short_name = 'spi_subform_2_taskstatus_spi_subform_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "spi_subform_2_taskstatus_spi_subform_4"
            END as "spi_subform_2_taskstatus_spi_subform_4",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data


  );