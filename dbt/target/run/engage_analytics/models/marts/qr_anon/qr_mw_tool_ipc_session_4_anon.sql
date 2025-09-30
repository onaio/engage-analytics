
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_mw_tool_ipc_session_4_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_mw_tool_ipc_session_4
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_mw_tool_ipc_session_4'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_mw_tool_ipc_session_4"
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
                    WHERE ("column" = 'mental_wellness_tool_in_the_past_year_have_there_been_times_w_3' OR linkid = 'mental_wellness_tool_in_the_past_year_have_there_been_times_w_3' OR short_name = 'mental_wellness_tool_in_the_past_year_have_there_been_times_w_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_in_the_past_year_have_there_been_times_w_3"
            END as "mental_wellness_tool_in_the_past_year_have_there_been_times_w_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_in_the_past_year_have_you_ever_felt_that_2' OR linkid = 'mental_wellness_tool_in_the_past_year_have_you_ever_felt_that_2' OR short_name = 'mental_wellness_tool_in_the_past_year_have_you_ever_felt_that_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_in_the_past_year_have_you_ever_felt_that_2"
            END as "mental_wellness_tool_in_the_past_year_have_you_ever_felt_that_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_in_the_past_year_how_often_do_you_have_a_2' OR linkid = 'mental_wellness_tool_in_the_past_year_how_often_do_you_have_a_2' OR short_name = 'mental_wellness_tool_in_the_past_year_how_often_do_you_have_a_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_in_the_past_year_how_often_do_you_have_a_2"
            END as "mental_wellness_tool_in_the_past_year_how_often_do_you_have_a_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_in_the_past_month_have_you_wished_you_we_2' OR linkid = 'mental_wellness_tool_in_the_past_month_have_you_wished_you_we_2' OR short_name = 'mental_wellness_tool_in_the_past_month_have_you_wished_you_we_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_in_the_past_month_have_you_wished_you_we_2"
            END as "mental_wellness_tool_in_the_past_month_have_you_wished_you_we_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_in_the_past_year_did_you_at_any_time_hea_2' OR linkid = 'mental_wellness_tool_in_the_past_year_did_you_at_any_time_hea_2' OR short_name = 'mental_wellness_tool_in_the_past_year_did_you_at_any_time_hea_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_in_the_past_year_did_you_at_any_time_hea_2"
            END as "mental_wellness_tool_in_the_past_year_did_you_at_any_time_hea_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_4' OR linkid = 'mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_4' OR short_name = 'mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_4"
            END as "mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_in_the_past_year_how_many_times_have_you_2' OR linkid = 'mental_wellness_tool_in_the_past_year_how_many_times_have_you_2' OR short_name = 'mental_wellness_tool_in_the_past_year_how_many_times_have_you_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_in_the_past_year_how_many_times_have_you_2"
            END as "mental_wellness_tool_in_the_past_year_how_many_times_have_you_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_in_the_past_year_how_many_drinks_contain_2' OR linkid = 'mental_wellness_tool_in_the_past_year_how_many_drinks_contain_2' OR short_name = 'mental_wellness_tool_in_the_past_year_how_many_drinks_contain_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_in_the_past_year_how_many_drinks_contain_2"
            END as "mental_wellness_tool_in_the_past_year_how_many_drinks_contain_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_in_the_past_3_months_have_you_ever_done__2' OR linkid = 'mental_wellness_tool_in_the_past_3_months_have_you_ever_done__2' OR short_name = 'mental_wellness_tool_in_the_past_3_months_have_you_ever_done__2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_in_the_past_3_months_have_you_ever_done__2"
            END as "mental_wellness_tool_in_the_past_3_months_have_you_ever_done__2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_5' OR linkid = 'mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_5' OR short_name = 'mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_5"
            END as "mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_in_the_past_month_have_you_had_any_actua_2' OR linkid = 'mental_wellness_tool_in_the_past_month_have_you_had_any_actua_2' OR short_name = 'mental_wellness_tool_in_the_past_month_have_you_had_any_actua_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_in_the_past_month_have_you_had_any_actua_2"
            END as "mental_wellness_tool_in_the_past_month_have_you_had_any_actua_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_alcohol_problem_2' OR linkid = 'mental_wellness_tool_alcohol_problem_2' OR short_name = 'mental_wellness_tool_alcohol_problem_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_alcohol_problem_2"
            END as "mental_wellness_tool_alcohol_problem_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_6' OR linkid = 'mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_6' OR short_name = 'mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_6"
            END as "mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_common_mental_health_2' OR linkid = 'mental_wellness_tool_common_mental_health_2' OR short_name = 'mental_wellness_tool_common_mental_health_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_common_mental_health_2"
            END as "mental_wellness_tool_common_mental_health_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_drug_problem_2' OR linkid = 'mental_wellness_tool_drug_problem_2' OR short_name = 'mental_wellness_tool_drug_problem_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_drug_problem_2"
            END as "mental_wellness_tool_drug_problem_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_in_the_past_year_have_there_been_times_w_4' OR linkid = 'mental_wellness_tool_in_the_past_year_have_there_been_times_w_4' OR short_name = 'mental_wellness_tool_in_the_past_year_have_there_been_times_w_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_in_the_past_year_have_there_been_times_w_4"
            END as "mental_wellness_tool_in_the_past_year_have_there_been_times_w_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_how_many_drinks_2' OR linkid = 'mental_wellness_tool_how_many_drinks_2' OR short_name = 'mental_wellness_tool_how_many_drinks_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_how_many_drinks_2"
            END as "mental_wellness_tool_how_many_drinks_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_no_mental_problem_2' OR linkid = 'mental_wellness_tool_no_mental_problem_2' OR short_name = 'mental_wellness_tool_no_mental_problem_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_no_mental_problem_2"
            END as "mental_wellness_tool_no_mental_problem_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'patient-age' OR linkid = 'patient-age' OR short_name = 'patient-age')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "patient-age"
            END as "patient-age",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'patient-biological-sex' OR linkid = 'patient-biological-sex' OR short_name = 'patient-biological-sex')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "patient-biological-sex"
            END as "patient-biological-sex",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'patient-dob' OR linkid = 'patient-dob' OR short_name = 'patient-dob')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "patient-dob"
            END as "patient-dob",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'patient-gender-identity' OR linkid = 'patient-gender-identity' OR short_name = 'patient-gender-identity')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "patient-gender-identity"
            END as "patient-gender-identity",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'patient-how-you-think-of-yourself' OR linkid = 'patient-how-you-think-of-yourself' OR short_name = 'patient-how-you-think-of-yourself')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "patient-how-you-think-of-yourself"
            END as "patient-how-you-think-of-yourself",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'patient-name' OR linkid = 'patient-name' OR short_name = 'patient-name')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "patient-name"
            END as "patient-name",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'patient-pronouns' OR linkid = 'patient-pronouns' OR short_name = 'patient-pronouns')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "patient-pronouns"
            END as "patient-pronouns",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_severe_mental_health_2' OR linkid = 'mental_wellness_tool_severe_mental_health_2' OR short_name = 'mental_wellness_tool_severe_mental_health_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_severe_mental_health_2"
            END as "mental_wellness_tool_severe_mental_health_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_suicide_risk_2' OR linkid = 'mental_wellness_tool_suicide_risk_2' OR short_name = 'mental_wellness_tool_suicide_risk_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_suicide_risk_2"
            END as "mental_wellness_tool_suicide_risk_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_taskid_of_pdf_2' OR linkid = 'mental_wellness_tool_taskid_of_pdf_2' OR short_name = 'mental_wellness_tool_taskid_of_pdf_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_taskid_of_pdf_2"
            END as "mental_wellness_tool_taskid_of_pdf_2",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data


  );