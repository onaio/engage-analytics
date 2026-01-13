

-- Anonymized view for qr_planning_next_steps
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_planning_next_steps'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_planning_next_steps"
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
                    WHERE ("column" = 'planning_next_stepsreferral_to_mental_health_specialist_where_w' OR linkid = 'planning_next_stepsreferral_to_mental_health_specialist_where_w' OR short_name = 'planning_next_stepsreferral_to_mental_health_specialist_where_w')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_stepsreferral_to_mental_health_specialist_where_w"
            END as "planning_next_stepsreferral_to_mental_health_specialist_where_w",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_stepsprobable_severe_mental_health_problem_additi' OR linkid = 'planning_next_stepsprobable_severe_mental_health_problem_additi' OR short_name = 'planning_next_stepsprobable_severe_mental_health_problem_additi')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_stepsprobable_severe_mental_health_problem_additi"
            END as "planning_next_stepsprobable_severe_mental_health_problem_additi",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_steps_additional_details' OR linkid = 'planning_next_steps_additional_details' OR short_name = 'planning_next_steps_additional_details')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_steps_additional_details"
            END as "planning_next_steps_additional_details",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_steps_did_you_discuss_this_client_s_scores_with_a' OR linkid = 'planning_next_steps_did_you_discuss_this_client_s_scores_with_a' OR short_name = 'planning_next_steps_did_you_discuss_this_client_s_scores_with_a')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_steps_did_you_discuss_this_client_s_scores_with_a"
            END as "planning_next_steps_did_you_discuss_this_client_s_scores_with_a",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_stepssafety_planning_intervention_spi_did_the_cli' OR linkid = 'planning_next_stepssafety_planning_intervention_spi_did_the_cli' OR short_name = 'planning_next_stepssafety_planning_intervention_spi_did_the_cli')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_stepssafety_planning_intervention_spi_did_the_cli"
            END as "planning_next_stepssafety_planning_intervention_spi_did_the_cli",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_stepsreferral_to_mental_health_specialist_additio' OR linkid = 'planning_next_stepsreferral_to_mental_health_specialist_additio' OR short_name = 'planning_next_stepsreferral_to_mental_health_specialist_additio')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_stepsreferral_to_mental_health_specialist_additio"
            END as "planning_next_stepsreferral_to_mental_health_specialist_additio",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_stepsother_agency_services_did_the_client_accept_' OR linkid = 'planning_next_stepsother_agency_services_did_the_client_accept_' OR short_name = 'planning_next_stepsother_agency_services_did_the_client_accept_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_stepsother_agency_services_did_the_client_accept_"
            END as "planning_next_stepsother_agency_services_did_the_client_accept_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_stepsinterpersonal_counseling_ipc_did_the_client_' OR linkid = 'planning_next_stepsinterpersonal_counseling_ipc_did_the_client_' OR short_name = 'planning_next_stepsinterpersonal_counseling_ipc_did_the_client_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_stepsinterpersonal_counseling_ipc_did_the_client_"
            END as "planning_next_stepsinterpersonal_counseling_ipc_did_the_client_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_stepsoptional_supports_is_the_client_interested_i' OR linkid = 'planning_next_stepsoptional_supports_is_the_client_interested_i' OR short_name = 'planning_next_stepsoptional_supports_is_the_client_interested_i')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_stepsoptional_supports_is_the_client_interested_i"
            END as "planning_next_stepsoptional_supports_is_the_client_interested_i",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_stepssafety_planning_intervention_spi_why_does_th' OR linkid = 'planning_next_stepssafety_planning_intervention_spi_why_does_th' OR short_name = 'planning_next_stepssafety_planning_intervention_spi_why_does_th')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_stepssafety_planning_intervention_spi_why_does_th"
            END as "planning_next_stepssafety_planning_intervention_spi_why_does_th",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_stepsother_agency_services_please_specify_which_o' OR linkid = 'planning_next_stepsother_agency_services_please_specify_which_o' OR short_name = 'planning_next_stepsother_agency_services_please_specify_which_o')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_stepsother_agency_services_please_specify_which_o"
            END as "planning_next_stepsother_agency_services_please_specify_which_o",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_stepsinterpersonal_counseling_ipc_why_does_the_cl' OR linkid = 'planning_next_stepsinterpersonal_counseling_ipc_why_does_the_cl' OR short_name = 'planning_next_stepsinterpersonal_counseling_ipc_why_does_the_cl')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_stepsinterpersonal_counseling_ipc_why_does_the_cl"
            END as "planning_next_stepsinterpersonal_counseling_ipc_why_does_the_cl",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_stepsother_agency_services_if_the_client_declined' OR linkid = 'planning_next_stepsother_agency_services_if_the_client_declined' OR short_name = 'planning_next_stepsother_agency_services_if_the_client_declined')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_stepsother_agency_services_if_the_client_declined"
            END as "planning_next_stepsother_agency_services_if_the_client_declined",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_stepsinterpersonal_counseling_ipc_schedule_ipc_se' OR linkid = 'planning_next_stepsinterpersonal_counseling_ipc_schedule_ipc_se' OR short_name = 'planning_next_stepsinterpersonal_counseling_ipc_schedule_ipc_se')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_stepsinterpersonal_counseling_ipc_schedule_ipc_se"
            END as "planning_next_stepsinterpersonal_counseling_ipc_schedule_ipc_se",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_stepsfinancial_hardships_did_the_client_accept_fi' OR linkid = 'planning_next_stepsfinancial_hardships_did_the_client_accept_fi' OR short_name = 'planning_next_stepsfinancial_hardships_did_the_client_accept_fi')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_stepsfinancial_hardships_did_the_client_accept_fi"
            END as "planning_next_stepsfinancial_hardships_did_the_client_accept_fi",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_steps_did_the_client_accept_spi' OR linkid = 'planning_next_steps_did_the_client_accept_spi' OR short_name = 'planning_next_steps_did_the_client_accept_spi')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_steps_did_the_client_accept_spi"
            END as "planning_next_steps_did_the_client_accept_spi",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_steps_please_specify' OR linkid = 'planning_next_steps_please_specify' OR short_name = 'planning_next_steps_please_specify')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_steps_please_specify"
            END as "planning_next_steps_please_specify",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_stepsprobable_common_mental_health_problem_schedu' OR linkid = 'planning_next_stepsprobable_common_mental_health_problem_schedu' OR short_name = 'planning_next_stepsprobable_common_mental_health_problem_schedu')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_stepsprobable_common_mental_health_problem_schedu"
            END as "planning_next_stepsprobable_common_mental_health_problem_schedu",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_stepsprobable_severe_mental_health_problem_did_th' OR linkid = 'planning_next_stepsprobable_severe_mental_health_problem_did_th' OR short_name = 'planning_next_stepsprobable_severe_mental_health_problem_did_th')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_stepsprobable_severe_mental_health_problem_did_th"
            END as "planning_next_stepsprobable_severe_mental_health_problem_did_th",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_steps_what_is_the_supervisor_s_recommendation_for' OR linkid = 'planning_next_steps_what_is_the_supervisor_s_recommendation_for' OR short_name = 'planning_next_steps_what_is_the_supervisor_s_recommendation_for')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_steps_what_is_the_supervisor_s_recommendation_for"
            END as "planning_next_steps_what_is_the_supervisor_s_recommendation_for",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_stepsother_services_outside_of_the_agency_did_the' OR linkid = 'planning_next_stepsother_services_outside_of_the_agency_did_the' OR short_name = 'planning_next_stepsother_services_outside_of_the_agency_did_the')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_stepsother_services_outside_of_the_agency_did_the"
            END as "planning_next_stepsother_services_outside_of_the_agency_did_the",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'additional_notes_additional_notes_or_information' OR linkid = 'additional_notes_additional_notes_or_information' OR short_name = 'additional_notes_additional_notes_or_information')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "additional_notes_additional_notes_or_information"
            END as "additional_notes_additional_notes_or_information",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_steps_did_the_client_complete_spi' OR linkid = 'planning_next_steps_did_the_client_complete_spi' OR short_name = 'planning_next_steps_did_the_client_complete_spi')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_steps_did_the_client_complete_spi"
            END as "planning_next_steps_did_the_client_complete_spi",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_stepsother_services_outside_of_the_agency_please_' OR linkid = 'planning_next_stepsother_services_outside_of_the_agency_please_' OR short_name = 'planning_next_stepsother_services_outside_of_the_agency_please_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_stepsother_services_outside_of_the_agency_please_"
            END as "planning_next_stepsother_services_outside_of_the_agency_please_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_stepssbirt_motivational_interviewing_did_the_clie' OR linkid = 'planning_next_stepssbirt_motivational_interviewing_did_the_clie' OR short_name = 'planning_next_stepssbirt_motivational_interviewing_did_the_clie')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_stepssbirt_motivational_interviewing_did_the_clie"
            END as "planning_next_stepssbirt_motivational_interviewing_did_the_clie",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_stepsprobable_common_mental_health_problem_did_th' OR linkid = 'planning_next_stepsprobable_common_mental_health_problem_did_th' OR short_name = 'planning_next_stepsprobable_common_mental_health_problem_did_th')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_stepsprobable_common_mental_health_problem_did_th"
            END as "planning_next_stepsprobable_common_mental_health_problem_did_th",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_steps_did_the_client_accept_sbirt' OR linkid = 'planning_next_steps_did_the_client_accept_sbirt' OR short_name = 'planning_next_steps_did_the_client_accept_sbirt')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_steps_did_the_client_accept_sbirt"
            END as "planning_next_steps_did_the_client_accept_sbirt",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_stepsother_services_outside_of_the_agency_if_the_' OR linkid = 'planning_next_stepsother_services_outside_of_the_agency_if_the_' OR short_name = 'planning_next_stepsother_services_outside_of_the_agency_if_the_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_stepsother_services_outside_of_the_agency_if_the_"
            END as "planning_next_stepsother_services_outside_of_the_agency_if_the_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_stepsfinancial_wellness_supports_did_the_client_a' OR linkid = 'planning_next_stepsfinancial_wellness_supports_did_the_client_a' OR short_name = 'planning_next_stepsfinancial_wellness_supports_did_the_client_a')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_stepsfinancial_wellness_supports_did_the_client_a"
            END as "planning_next_stepsfinancial_wellness_supports_did_the_client_a",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_stepssbirt_motivational_interviewing_why_does_the' OR linkid = 'planning_next_stepssbirt_motivational_interviewing_why_does_the' OR short_name = 'planning_next_stepssbirt_motivational_interviewing_why_does_the')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_stepssbirt_motivational_interviewing_why_does_the"
            END as "planning_next_stepssbirt_motivational_interviewing_why_does_the",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_steps_schedule_sbirt_session' OR linkid = 'planning_next_steps_schedule_sbirt_session' OR short_name = 'planning_next_steps_schedule_sbirt_session')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_steps_schedule_sbirt_session"
            END as "planning_next_steps_schedule_sbirt_session",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_steps_if_the_client_declined_please_indicate_the_' OR linkid = 'planning_next_steps_if_the_client_declined_please_indicate_the_' OR short_name = 'planning_next_steps_if_the_client_declined_please_indicate_the_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_steps_if_the_client_declined_please_indicate_the_"
            END as "planning_next_steps_if_the_client_declined_please_indicate_the_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_stepsfinancial_wellness_supports_why_does_the_cli' OR linkid = 'planning_next_stepsfinancial_wellness_supports_why_does_the_cli' OR short_name = 'planning_next_stepsfinancial_wellness_supports_why_does_the_cli')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_stepsfinancial_wellness_supports_why_does_the_cli"
            END as "planning_next_stepsfinancial_wellness_supports_why_does_the_cli",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_stepssbirt_motivational_interviewing_schedule_sbi' OR linkid = 'planning_next_stepssbirt_motivational_interviewing_schedule_sbi' OR short_name = 'planning_next_stepssbirt_motivational_interviewing_schedule_sbi')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_stepssbirt_motivational_interviewing_schedule_sbi"
            END as "planning_next_stepssbirt_motivational_interviewing_schedule_sbi",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_stepsprobable_common_mental_health_problem_if_the' OR linkid = 'planning_next_stepsprobable_common_mental_health_problem_if_the' OR short_name = 'planning_next_stepsprobable_common_mental_health_problem_if_the')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_stepsprobable_common_mental_health_problem_if_the"
            END as "planning_next_stepsprobable_common_mental_health_problem_if_the",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_stepsprobable_severe_mental_health_problem_where_' OR linkid = 'planning_next_stepsprobable_severe_mental_health_problem_where_' OR short_name = 'planning_next_stepsprobable_severe_mental_health_problem_where_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_stepsprobable_severe_mental_health_problem_where_"
            END as "planning_next_stepsprobable_severe_mental_health_problem_where_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_stepsfinancial_wellness_supports_if_the_client_de' OR linkid = 'planning_next_stepsfinancial_wellness_supports_if_the_client_de' OR short_name = 'planning_next_stepsfinancial_wellness_supports_if_the_client_de')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_stepsfinancial_wellness_supports_if_the_client_de"
            END as "planning_next_stepsfinancial_wellness_supports_if_the_client_de",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_stepsreferral_to_mental_health_specialist_did_the' OR linkid = 'planning_next_stepsreferral_to_mental_health_specialist_did_the' OR short_name = 'planning_next_stepsreferral_to_mental_health_specialist_did_the')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_stepsreferral_to_mental_health_specialist_did_the"
            END as "planning_next_stepsreferral_to_mental_health_specialist_did_the",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_stepsreferral_to_mental_health_specialist_why_doe' OR linkid = 'planning_next_stepsreferral_to_mental_health_specialist_why_doe' OR short_name = 'planning_next_stepsreferral_to_mental_health_specialist_why_doe')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_stepsreferral_to_mental_health_specialist_why_doe"
            END as "planning_next_stepsreferral_to_mental_health_specialist_why_doe",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_steps_if_the_client_declined_please_indicate_th_2' OR linkid = 'planning_next_steps_if_the_client_declined_please_indicate_th_2' OR short_name = 'planning_next_steps_if_the_client_declined_please_indicate_th_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_steps_if_the_client_declined_please_indicate_th_2"
            END as "planning_next_steps_if_the_client_declined_please_indicate_th_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_steps_has_the_supervisor_assessed_the_client_to_d' OR linkid = 'planning_next_steps_has_the_supervisor_assessed_the_client_to_d' OR short_name = 'planning_next_steps_has_the_supervisor_assessed_the_client_to_d')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_steps_has_the_supervisor_assessed_the_client_to_d"
            END as "planning_next_steps_has_the_supervisor_assessed_the_client_to_d",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'discussion_with_supervisor_for_severe_mental_health_superviso_2' OR linkid = 'discussion_with_supervisor_for_severe_mental_health_superviso_2' OR short_name = 'discussion_with_supervisor_for_severe_mental_health_superviso_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "discussion_with_supervisor_for_severe_mental_health_superviso_2"
            END as "discussion_with_supervisor_for_severe_mental_health_superviso_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_steps_financial_hardships_list' OR linkid = 'planning_next_steps_financial_hardships_list' OR short_name = 'planning_next_steps_financial_hardships_list')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_steps_financial_hardships_list"
            END as "planning_next_steps_financial_hardships_list",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'is_fws' OR linkid = 'is_fws' OR short_name = 'is_fws')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "is_fws"
            END as "is_fws",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'is_ipc' OR linkid = 'is_ipc' OR short_name = 'is_ipc')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "is_ipc"
            END as "is_ipc",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'is_ipc_main' OR linkid = 'is_ipc_main' OR short_name = 'is_ipc_main')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "is_ipc_main"
            END as "is_ipc_main",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'is_ipc_optional' OR linkid = 'is_ipc_optional' OR short_name = 'is_ipc_optional')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "is_ipc_optional"
            END as "is_ipc_optional",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'is_mhs' OR linkid = 'is_mhs' OR short_name = 'is_mhs')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "is_mhs"
            END as "is_mhs",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'is_other_agency_services' OR linkid = 'is_other_agency_services' OR short_name = 'is_other_agency_services')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "is_other_agency_services"
            END as "is_other_agency_services",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'is_other_services_outside_agency' OR linkid = 'is_other_services_outside_agency' OR short_name = 'is_other_services_outside_agency')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "is_other_services_outside_agency"
            END as "is_other_services_outside_agency",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'is_sbirt' OR linkid = 'is_sbirt' OR short_name = 'is_sbirt')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "is_sbirt"
            END as "is_sbirt",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'is_sbirt_main' OR linkid = 'is_sbirt_main' OR short_name = 'is_sbirt_main')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "is_sbirt_main"
            END as "is_sbirt_main",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'is_sbirt_optional' OR linkid = 'is_sbirt_optional' OR short_name = 'is_sbirt_optional')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "is_sbirt_optional"
            END as "is_sbirt_optional",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'is_spi' OR linkid = 'is_spi' OR short_name = 'is_spi')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "is_spi"
            END as "is_spi",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'patient_age_4' OR linkid = 'patient_age_4' OR short_name = 'patient_age_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "patient_age_4"
            END as "patient_age_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'patient_date_of_birth_4' OR linkid = 'patient_date_of_birth_4' OR short_name = 'patient_date_of_birth_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "patient_date_of_birth_4"
            END as "patient_date_of_birth_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'patient_name_4' OR linkid = 'patient_name_4' OR short_name = 'patient_name_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "patient_name_4"
            END as "patient_name_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_steps_scheduled_ipc_session_1_with_current_time' OR linkid = 'planning_next_steps_scheduled_ipc_session_1_with_current_time' OR short_name = 'planning_next_steps_scheduled_ipc_session_1_with_current_time')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_steps_scheduled_ipc_session_1_with_current_time"
            END as "planning_next_steps_scheduled_ipc_session_1_with_current_time",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'scheduled_ipc_session_1_with_current_time_2' OR linkid = 'scheduled_ipc_session_1_with_current_time_2' OR short_name = 'scheduled_ipc_session_1_with_current_time_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "scheduled_ipc_session_1_with_current_time_2"
            END as "scheduled_ipc_session_1_with_current_time_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_steps_taskid_000' OR linkid = 'planning_next_steps_taskid_000' OR short_name = 'planning_next_steps_taskid_000')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_steps_taskid_000"
            END as "planning_next_steps_taskid_000",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_steps_taskid_001' OR linkid = 'planning_next_steps_taskid_001' OR short_name = 'planning_next_steps_taskid_001')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_steps_taskid_001"
            END as "planning_next_steps_taskid_001",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_steps_taskid_002' OR linkid = 'planning_next_steps_taskid_002' OR short_name = 'planning_next_steps_taskid_002')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_steps_taskid_002"
            END as "planning_next_steps_taskid_002",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_steps_taskid_003' OR linkid = 'planning_next_steps_taskid_003' OR short_name = 'planning_next_steps_taskid_003')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_steps_taskid_003"
            END as "planning_next_steps_taskid_003",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_steps_taskid_004' OR linkid = 'planning_next_steps_taskid_004' OR short_name = 'planning_next_steps_taskid_004')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_steps_taskid_004"
            END as "planning_next_steps_taskid_004",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_steps_taskid_006' OR linkid = 'planning_next_steps_taskid_006' OR short_name = 'planning_next_steps_taskid_006')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_steps_taskid_006"
            END as "planning_next_steps_taskid_006",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'planning_next_steps_taskid_pdf' OR linkid = 'planning_next_steps_taskid_pdf' OR short_name = 'planning_next_steps_taskid_pdf')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "planning_next_steps_taskid_pdf"
            END as "planning_next_steps_taskid_pdf",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data

