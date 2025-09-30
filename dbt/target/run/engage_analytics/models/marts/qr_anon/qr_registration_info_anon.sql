
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_registration_info_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_registration_info
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_registration_info'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_registration_info"
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
                    WHERE ("column" = 'add_family_member_registration' OR linkid = 'add_family_member_registration' OR short_name = 'add_family_member_registration')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration"
            END as "add_family_member_registration",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_middle_name' OR linkid = 'add_family_member_registration_middle_name' OR short_name = 'add_family_member_registration_middle_name')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_middle_name"
            END as "add_family_member_registration_middle_name",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_which_of_the_following_best_repr' OR linkid = 'add_family_member_registration_which_of_the_following_best_repr' OR short_name = 'add_family_member_registration_which_of_the_following_best_repr')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_which_of_the_following_best_repr"
            END as "add_family_member_registration_which_of_the_following_best_repr",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_age_estimate_if_unknown' OR linkid = 'add_family_member_registration_age_estimate_if_unknown' OR short_name = 'add_family_member_registration_age_estimate_if_unknown')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_age_estimate_if_unknown"
            END as "add_family_member_registration_age_estimate_if_unknown",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_last_name' OR linkid = 'add_family_member_registration_last_name' OR short_name = 'add_family_member_registration_last_name')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_last_name"
            END as "add_family_member_registration_last_name",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'age' OR linkid = 'age' OR short_name = 'age')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "age"
            END as "age",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_age_6' OR linkid = 'add_family_member_registration_calculated_age_6' OR short_name = 'add_family_member_registration_calculated_age_6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_age_6"
            END as "add_family_member_registration_calculated_age_6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_unique_id' OR linkid = 'add_family_member_registration_unique_id' OR short_name = 'add_family_member_registration_unique_id')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_unique_id"
            END as "add_family_member_registration_unique_id",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_have_you_received_engage_service' OR linkid = 'add_family_member_registration_have_you_received_engage_service' OR short_name = 'add_family_member_registration_have_you_received_engage_service')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_have_you_received_engage_service"
            END as "add_family_member_registration_have_you_received_engage_service",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_month_6' OR linkid = 'add_family_member_registration_calculated_month_6' OR short_name = 'add_family_member_registration_calculated_month_6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_month_6"
            END as "add_family_member_registration_calculated_month_6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_calculated_year_6' OR linkid = 'add_family_member_registration_calculated_year_6' OR short_name = 'add_family_member_registration_calculated_year_6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_calculated_year_6"
            END as "add_family_member_registration_calculated_year_6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_date_of_birth_6' OR linkid = 'add_family_member_registration_date_of_birth_6' OR short_name = 'add_family_member_registration_date_of_birth_6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_date_of_birth_6"
            END as "add_family_member_registration_date_of_birth_6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_what_term_best_expresses_how_you' OR linkid = 'add_family_member_registration_what_term_best_expresses_how_you' OR short_name = 'add_family_member_registration_what_term_best_expresses_how_you')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_what_term_best_expresses_how_you"
            END as "add_family_member_registration_what_term_best_expresses_how_you",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_what_are_your_preferred_pronouns' OR linkid = 'add_family_member_registration_what_are_your_preferred_pronouns' OR short_name = 'add_family_member_registration_what_are_your_preferred_pronouns')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_what_are_your_preferred_pronouns"
            END as "add_family_member_registration_what_are_your_preferred_pronouns",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_do_you_have_medicaid' OR linkid = 'add_family_member_registration_do_you_have_medicaid' OR short_name = 'add_family_member_registration_do_you_have_medicaid')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_do_you_have_medicaid"
            END as "add_family_member_registration_do_you_have_medicaid",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_first_name' OR linkid = 'add_family_member_registration_first_name' OR short_name = 'add_family_member_registration_first_name')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_first_name"
            END as "add_family_member_registration_first_name",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_email_address' OR linkid = 'add_family_member_registration_email_address' OR short_name = 'add_family_member_registration_email_address')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_email_address"
            END as "add_family_member_registration_email_address",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_what_was_your_biological_sex_at_' OR linkid = 'add_family_member_registration_what_was_your_biological_sex_at_' OR short_name = 'add_family_member_registration_what_was_your_biological_sex_at_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_what_was_your_biological_sex_at_"
            END as "add_family_member_registration_what_was_your_biological_sex_at_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_medicaid_number' OR linkid = 'add_family_member_registration_medicaid_number' OR short_name = 'add_family_member_registration_medicaid_number')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_medicaid_number"
            END as "add_family_member_registration_medicaid_number",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_2' OR linkid = 'add_family_member_registration_2' OR short_name = 'add_family_member_registration_2')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_2"
            END as "add_family_member_registration_2",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_3' OR linkid = 'add_family_member_registration_3' OR short_name = 'add_family_member_registration_3')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_3"
            END as "add_family_member_registration_3",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_4' OR linkid = 'add_family_member_registration_4' OR short_name = 'add_family_member_registration_4')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_4"
            END as "add_family_member_registration_4",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_5' OR linkid = 'add_family_member_registration_5' OR short_name = 'add_family_member_registration_5')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_5"
            END as "add_family_member_registration_5",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_6' OR linkid = 'add_family_member_registration_6' OR short_name = 'add_family_member_registration_6')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_6"
            END as "add_family_member_registration_6",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_phone_number' OR linkid = 'add_family_member_registration_phone_number' OR short_name = 'add_family_member_registration_phone_number')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        CASE
                            WHEN "add_family_member_registration_phone_number" IS NOT NULL AND LENGTH("add_family_member_registration_phone_number"::text) >= 4
                            THEN 'XXX-XXX-' || RIGHT("add_family_member_registration_phone_number"::text, 4)
                            ELSE 'NO PHONE'
                        END
                    
                ELSE "add_family_member_registration_phone_number"
            END as "add_family_member_registration_phone_number",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_physical_address' OR linkid = 'add_family_member_registration_physical_address' OR short_name = 'add_family_member_registration_physical_address')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_physical_address"
            END as "add_family_member_registration_physical_address",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_where_did_you_first_learn_about_' OR linkid = 'add_family_member_registration_where_did_you_first_learn_about_' OR short_name = 'add_family_member_registration_where_did_you_first_learn_about_')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "add_family_member_registration_where_did_you_first_learn_about_"
            END as "add_family_member_registration_where_did_you_first_learn_about_",
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'add_family_member_registration_zip_code' OR linkid = 'add_family_member_registration_zip_code' OR short_name = 'add_family_member_registration_zip_code')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        CASE
                            WHEN "add_family_member_registration_zip_code" IS NOT NULL
                            THEN LEFT("add_family_member_registration_zip_code"::text, 3) || 'XX'
                            ELSE NULL
                        END
                    
                ELSE "add_family_member_registration_zip_code"
            END as "add_family_member_registration_zip_code",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data


  );