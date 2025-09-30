
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_mw_tool_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_mw_tool
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_mw_tool'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_mw_tool"
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
                    WHERE ("column" = 'mental_wellness_tool_in_the_past_year_have_there_been_times_whe' OR linkid = 'mental_wellness_tool_in_the_past_year_have_there_been_times_whe' OR short_name = 'mental_wellness_tool_in_the_past_year_have_there_been_times_whe')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_in_the_past_year_have_there_been_times_whe"
            END as "mental_wellness_tool_in_the_past_year_have_there_been_times_whe",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_in_the_past_year_have_you_ever_felt_that_y' OR linkid = 'mental_wellness_tool_in_the_past_year_have_you_ever_felt_that_y' OR short_name = 'mental_wellness_tool_in_the_past_year_have_you_ever_felt_that_y')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_in_the_past_year_have_you_ever_felt_that_y"
            END as "mental_wellness_tool_in_the_past_year_have_you_ever_felt_that_y",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_in_the_past_year_how_often_do_you_have_a_d' OR linkid = 'mental_wellness_tool_in_the_past_year_how_often_do_you_have_a_d' OR short_name = 'mental_wellness_tool_in_the_past_year_how_often_do_you_have_a_d')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_in_the_past_year_how_often_do_you_have_a_d"
            END as "mental_wellness_tool_in_the_past_year_how_often_do_you_have_a_d",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_in_the_past_month_have_you_wished_you_were' OR linkid = 'mental_wellness_tool_in_the_past_month_have_you_wished_you_were' OR short_name = 'mental_wellness_tool_in_the_past_month_have_you_wished_you_were')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_in_the_past_month_have_you_wished_you_were"
            END as "mental_wellness_tool_in_the_past_month_have_you_wished_you_were",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_in_the_past_year_did_you_at_any_time_hear_' OR linkid = 'mental_wellness_tool_in_the_past_year_did_you_at_any_time_hear_' OR short_name = 'mental_wellness_tool_in_the_past_year_did_you_at_any_time_hear_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_in_the_past_year_did_you_at_any_time_hear_"
            END as "mental_wellness_tool_in_the_past_year_did_you_at_any_time_hear_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'discussion_with_supervisor_for_severe_mental_health_did_you_dis' OR linkid = 'discussion_with_supervisor_for_severe_mental_health_did_you_dis' OR short_name = 'discussion_with_supervisor_for_severe_mental_health_did_you_dis')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "discussion_with_supervisor_for_severe_mental_health_did_you_dis"
            END as "discussion_with_supervisor_for_severe_mental_health_did_you_dis",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'discussion_with_supervisor_for_severe_mental_health_what_is_the' OR linkid = 'discussion_with_supervisor_for_severe_mental_health_what_is_the' OR short_name = 'discussion_with_supervisor_for_severe_mental_health_what_is_the')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "discussion_with_supervisor_for_severe_mental_health_what_is_the"
            END as "discussion_with_supervisor_for_severe_mental_health_what_is_the",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_bee' OR linkid = 'mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_bee' OR short_name = 'mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_bee')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_bee"
            END as "mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_bee",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_in_the_past_year_how_many_times_have_you_u' OR linkid = 'mental_wellness_tool_in_the_past_year_how_many_times_have_you_u' OR short_name = 'mental_wellness_tool_in_the_past_year_how_many_times_have_you_u')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_in_the_past_year_how_many_times_have_you_u"
            END as "mental_wellness_tool_in_the_past_year_how_many_times_have_you_u",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'discussion_with_supervisor_for_incomplete_answers_since_respons' OR linkid = 'discussion_with_supervisor_for_incomplete_answers_since_respons' OR short_name = 'discussion_with_supervisor_for_incomplete_answers_since_respons')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "discussion_with_supervisor_for_incomplete_answers_since_respons"
            END as "discussion_with_supervisor_for_incomplete_answers_since_respons",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'discussion_with_supervisor_for_incomplete_answers_since_respo_2' OR linkid = 'discussion_with_supervisor_for_incomplete_answers_since_respo_2' OR short_name = 'discussion_with_supervisor_for_incomplete_answers_since_respo_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "discussion_with_supervisor_for_incomplete_answers_since_respo_2"
            END as "discussion_with_supervisor_for_incomplete_answers_since_respo_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'discussion_with_supervisor_for_severe_mental_health_referral_de' OR linkid = 'discussion_with_supervisor_for_severe_mental_health_referral_de' OR short_name = 'discussion_with_supervisor_for_severe_mental_health_referral_de')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "discussion_with_supervisor_for_severe_mental_health_referral_de"
            END as "discussion_with_supervisor_for_severe_mental_health_referral_de",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_in_the_past_year_how_many_drinks_containin' OR linkid = 'mental_wellness_tool_in_the_past_year_how_many_drinks_containin' OR short_name = 'mental_wellness_tool_in_the_past_year_how_many_drinks_containin')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_in_the_past_year_how_many_drinks_containin"
            END as "mental_wellness_tool_in_the_past_year_how_many_drinks_containin",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_in_the_past_3_months_have_you_ever_done_an' OR linkid = 'mental_wellness_tool_in_the_past_3_months_have_you_ever_done_an' OR short_name = 'mental_wellness_tool_in_the_past_3_months_have_you_ever_done_an')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_in_the_past_3_months_have_you_ever_done_an"
            END as "mental_wellness_tool_in_the_past_3_months_have_you_ever_done_an",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_2' OR linkid = 'mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_2' OR short_name = 'mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_2"
            END as "mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_in_the_past_month_have_you_had_any_actual_' OR linkid = 'mental_wellness_tool_in_the_past_month_have_you_had_any_actual_' OR short_name = 'mental_wellness_tool_in_the_past_month_have_you_had_any_actual_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_in_the_past_month_have_you_had_any_actual_"
            END as "mental_wellness_tool_in_the_past_month_have_you_had_any_actual_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_alcohol_problem' OR linkid = 'mental_wellness_tool_alcohol_problem' OR short_name = 'mental_wellness_tool_alcohol_problem')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_alcohol_problem"
            END as "mental_wellness_tool_alcohol_problem",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_3' OR linkid = 'mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_3' OR short_name = 'mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_3"
            END as "mental_wellness_tool_in_the_last_2_weeks_how_often_have_you_b_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_common_mental_health' OR linkid = 'mental_wellness_tool_common_mental_health' OR short_name = 'mental_wellness_tool_common_mental_health')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_common_mental_health"
            END as "mental_wellness_tool_common_mental_health",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_drug_problem' OR linkid = 'mental_wellness_tool_drug_problem' OR short_name = 'mental_wellness_tool_drug_problem')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_drug_problem"
            END as "mental_wellness_tool_drug_problem",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_in_the_past_year_have_there_been_times_w_2' OR linkid = 'mental_wellness_tool_in_the_past_year_have_there_been_times_w_2' OR short_name = 'mental_wellness_tool_in_the_past_year_have_there_been_times_w_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_in_the_past_year_have_there_been_times_w_2"
            END as "mental_wellness_tool_in_the_past_year_have_there_been_times_w_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'discussion_with_supervisor_for_severe_mental_health_supervisor_' OR linkid = 'discussion_with_supervisor_for_severe_mental_health_supervisor_' OR short_name = 'discussion_with_supervisor_for_severe_mental_health_supervisor_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "discussion_with_supervisor_for_severe_mental_health_supervisor_"
            END as "discussion_with_supervisor_for_severe_mental_health_supervisor_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_how_many_drinks' OR linkid = 'mental_wellness_tool_how_many_drinks' OR short_name = 'mental_wellness_tool_how_many_drinks')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_how_many_drinks"
            END as "mental_wellness_tool_how_many_drinks",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_no_mental_problem' OR linkid = 'mental_wellness_tool_no_mental_problem' OR short_name = 'mental_wellness_tool_no_mental_problem')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_no_mental_problem"
            END as "mental_wellness_tool_no_mental_problem",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'patient_age_2' OR linkid = 'patient_age_2' OR short_name = 'patient_age_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "patient_age_2"
            END as "patient_age_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'patient_biological_sex' OR linkid = 'patient_biological_sex' OR short_name = 'patient_biological_sex')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "patient_biological_sex"
            END as "patient_biological_sex",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'patient_date_of_birth_2' OR linkid = 'patient_date_of_birth_2' OR short_name = 'patient_date_of_birth_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "patient_date_of_birth_2"
            END as "patient_date_of_birth_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'patient_gender_identity' OR linkid = 'patient_gender_identity' OR short_name = 'patient_gender_identity')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "patient_gender_identity"
            END as "patient_gender_identity",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'patient_how_you_think_of_yourself' OR linkid = 'patient_how_you_think_of_yourself' OR short_name = 'patient_how_you_think_of_yourself')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "patient_how_you_think_of_yourself"
            END as "patient_how_you_think_of_yourself",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'patient_name_2' OR linkid = 'patient_name_2' OR short_name = 'patient_name_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "patient_name_2"
            END as "patient_name_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'patient_pronouns' OR linkid = 'patient_pronouns' OR short_name = 'patient_pronouns')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "patient_pronouns"
            END as "patient_pronouns",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'the_client_s_mental_wellness_tool_results_q1_to_q3_negative' OR linkid = 'the_client_s_mental_wellness_tool_results_q1_to_q3_negative' OR short_name = 'the_client_s_mental_wellness_tool_results_q1_to_q3_negative')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "the_client_s_mental_wellness_tool_results_q1_to_q3_negative"
            END as "the_client_s_mental_wellness_tool_results_q1_to_q3_negative",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_severe_mental_health' OR linkid = 'mental_wellness_tool_severe_mental_health' OR short_name = 'mental_wellness_tool_severe_mental_health')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_severe_mental_health"
            END as "mental_wellness_tool_severe_mental_health",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_suicide_risk' OR linkid = 'mental_wellness_tool_suicide_risk' OR short_name = 'mental_wellness_tool_suicide_risk')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_suicide_risk"
            END as "mental_wellness_tool_suicide_risk",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_taskid_of_common_mental_health_form' OR linkid = 'mental_wellness_tool_taskid_of_common_mental_health_form' OR short_name = 'mental_wellness_tool_taskid_of_common_mental_health_form')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_taskid_of_common_mental_health_form"
            END as "mental_wellness_tool_taskid_of_common_mental_health_form",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'mental_wellness_tool_taskid_of_pdf' OR linkid = 'mental_wellness_tool_taskid_of_pdf' OR short_name = 'mental_wellness_tool_taskid_of_pdf')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "mental_wellness_tool_taskid_of_pdf"
            END as "mental_wellness_tool_taskid_of_pdf",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'task_id_socio_pdf' OR linkid = 'task_id_socio_pdf' OR short_name = 'task_id_socio_pdf')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "task_id_socio_pdf"
            END as "task_id_socio_pdf",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data


  );