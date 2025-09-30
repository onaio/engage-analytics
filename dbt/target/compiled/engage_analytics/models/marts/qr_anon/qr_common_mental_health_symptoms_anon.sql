

-- Anonymized view for qr_common_mental_health_symptoms
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_common_mental_health_symptoms'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_common_mental_health_symptoms"
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
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirpath' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirpath' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirpath')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirpath"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirpath",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_wer' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_wer' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_wer')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_wer"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_wer",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirpa_2' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirpa_2' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirpa_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirpa_2"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirpa_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_2' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_2' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_2"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirpa_3' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirpa_3' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirpa_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirpa_3"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirpa_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_3' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_3' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_3"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_4' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_4' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_4"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_5' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_5' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_5"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirpa_4' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirpa_4' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirpa_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirpa_4"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirpa_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'phq_9_ipc_session_2_how_difficult_have_these_problems_made_it_f' OR linkid = 'phq_9_ipc_session_2_how_difficult_have_these_problems_made_it_f' OR short_name = 'phq_9_ipc_session_2_how_difficult_have_these_problems_made_it_f')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "phq_9_ipc_session_2_how_difficult_have_these_problems_made_it_f"
            END as "phq_9_ipc_session_2_how_difficult_have_these_problems_made_it_f",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirpa_5' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirpa_5' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirpa_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirpa_5"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirpa_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_6' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_6' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_6"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirpa_6' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirpa_6' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirpa_6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirpa_6"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirpa_6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_7' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_7' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_7')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_7"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_7",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_8' OR linkid = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_8' OR short_name = 'short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_8')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_8"
            END as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much_w_8",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_moving_or_speaking_so_slowly_that' OR linkid = 'common_mental_health_symptoms_moving_or_speaking_so_slowly_that' OR short_name = 'common_mental_health_symptoms_moving_or_speaking_so_slowly_that')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_moving_or_speaking_so_slowly_that"
            END as "common_mental_health_symptoms_moving_or_speaking_so_slowly_that",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_thoughts_that_you_would_be_better' OR linkid = 'common_mental_health_symptoms_thoughts_that_you_would_be_better' OR short_name = 'common_mental_health_symptoms_thoughts_that_you_would_be_better')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_thoughts_that_you_would_be_better"
            END as "common_mental_health_symptoms_thoughts_that_you_would_be_better",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_feeling_nervous_anxious_or_on_edg' OR linkid = 'common_mental_health_symptoms_feeling_nervous_anxious_or_on_edg' OR short_name = 'common_mental_health_symptoms_feeling_nervous_anxious_or_on_edg')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_feeling_nervous_anxious_or_on_edg"
            END as "common_mental_health_symptoms_feeling_nervous_anxious_or_on_edg",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_not_being_able_to_stop_or_control' OR linkid = 'common_mental_health_symptoms_not_being_able_to_stop_or_control' OR short_name = 'common_mental_health_symptoms_not_being_able_to_stop_or_control')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_not_being_able_to_stop_or_control"
            END as "common_mental_health_symptoms_not_being_able_to_stop_or_control",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_worrying_too_much_about_different' OR linkid = 'common_mental_health_symptoms_worrying_too_much_about_different' OR short_name = 'common_mental_health_symptoms_worrying_too_much_about_different')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_worrying_too_much_about_different"
            END as "common_mental_health_symptoms_worrying_too_much_about_different",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_trouble_relaxing' OR linkid = 'common_mental_health_symptoms_trouble_relaxing' OR short_name = 'common_mental_health_symptoms_trouble_relaxing')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_trouble_relaxing"
            END as "common_mental_health_symptoms_trouble_relaxing",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_being_so_restless_that_it_s_hard_' OR linkid = 'common_mental_health_symptoms_being_so_restless_that_it_s_hard_' OR short_name = 'common_mental_health_symptoms_being_so_restless_that_it_s_hard_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_being_so_restless_that_it_s_hard_"
            END as "common_mental_health_symptoms_being_so_restless_that_it_s_hard_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_becoming_easily_annoyed_or_irrita' OR linkid = 'common_mental_health_symptoms_becoming_easily_annoyed_or_irrita' OR short_name = 'common_mental_health_symptoms_becoming_easily_annoyed_or_irrita')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_becoming_easily_annoyed_or_irrita"
            END as "common_mental_health_symptoms_becoming_easily_annoyed_or_irrita",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_feeling_afraid_as_if_something_aw' OR linkid = 'common_mental_health_symptoms_feeling_afraid_as_if_something_aw' OR short_name = 'common_mental_health_symptoms_feeling_afraid_as_if_something_aw')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_feeling_afraid_as_if_something_aw"
            END as "common_mental_health_symptoms_feeling_afraid_as_if_something_aw",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_little_interest_or_pleasure_in_do' OR linkid = 'common_mental_health_symptoms_little_interest_or_pleasure_in_do' OR short_name = 'common_mental_health_symptoms_little_interest_or_pleasure_in_do')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_little_interest_or_pleasure_in_do"
            END as "common_mental_health_symptoms_little_interest_or_pleasure_in_do",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_feeling_down_depressed_or_hopeles' OR linkid = 'common_mental_health_symptoms_feeling_down_depressed_or_hopeles' OR short_name = 'common_mental_health_symptoms_feeling_down_depressed_or_hopeles')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_feeling_down_depressed_or_hopeles"
            END as "common_mental_health_symptoms_feeling_down_depressed_or_hopeles",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_trouble_falling_or_staying_asleep' OR linkid = 'common_mental_health_symptoms_trouble_falling_or_staying_asleep' OR short_name = 'common_mental_health_symptoms_trouble_falling_or_staying_asleep')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_trouble_falling_or_staying_asleep"
            END as "common_mental_health_symptoms_trouble_falling_or_staying_asleep",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_feeling_tired_or_having_little_en' OR linkid = 'common_mental_health_symptoms_feeling_tired_or_having_little_en' OR short_name = 'common_mental_health_symptoms_feeling_tired_or_having_little_en')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_feeling_tired_or_having_little_en"
            END as "common_mental_health_symptoms_feeling_tired_or_having_little_en",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_poor_appetite_or_overeating' OR linkid = 'common_mental_health_symptoms_poor_appetite_or_overeating' OR short_name = 'common_mental_health_symptoms_poor_appetite_or_overeating')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_poor_appetite_or_overeating"
            END as "common_mental_health_symptoms_poor_appetite_or_overeating",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_feeling_bad_about_yourself_or_tha' OR linkid = 'common_mental_health_symptoms_feeling_bad_about_yourself_or_tha' OR short_name = 'common_mental_health_symptoms_feeling_bad_about_yourself_or_tha')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_feeling_bad_about_yourself_or_tha"
            END as "common_mental_health_symptoms_feeling_bad_about_yourself_or_tha",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'common_mental_health_symptoms_trouble_concentrating_on_things_s' OR linkid = 'common_mental_health_symptoms_trouble_concentrating_on_things_s' OR short_name = 'common_mental_health_symptoms_trouble_concentrating_on_things_s')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "common_mental_health_symptoms_trouble_concentrating_on_things_s"
            END as "common_mental_health_symptoms_trouble_concentrating_on_things_s",
        
    
        
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
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirpa_7' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirpa_7' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirpa_7')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirpa_7"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirpa_7",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirpa_8' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirpa_8' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirpa_8')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirpa_8"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirpa_8",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data

