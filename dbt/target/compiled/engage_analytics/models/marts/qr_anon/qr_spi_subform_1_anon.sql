

-- Anonymized view for qr_spi_subform_1 with PII fields masked based on questionnaire_metadata.anon flag
-- Auto-generated to mask fields marked as anon=TRUE in questionnaire_metadata

with metadata_pii as (
    select distinct
        question_alias
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where questionnaire_id in ('Questionnaire/104455')
    and anon = 'TRUE'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_spi_subform_1"
)

select 
    MD5(COALESCE(qr_id, '')::text) as qr_id_hash,
    questionnaire_id,
    MD5(COALESCE(subject_patient_id, '')::text) as subject_patient_id_hash,
    encounter_id,
    author_practitioner_id,
    practitioner_location_id,
    practitioner_organization_id,
    practitioner_id,
    practitioner_careteam_id,
    application_version,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_encounter_id_of_spi_sub_1')
        THEN 'REDACTED'
        ELSE spi_encounter_id_of_spi_sub_1::text
    END as spi_encounter_id_of_spi_sub_1,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_is_low_risk')
        THEN 'REDACTED'
        ELSE spi_is_low_risk::text
    END as spi_is_low_risk,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_is_medium_risk')
        THEN 'REDACTED'
        ELSE spi_is_medium_risk::text
    END as spi_is_medium_risk,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_is_high_risk')
        THEN 'REDACTED'
        ELSE spi_is_high_risk::text
    END as spi_is_high_risk,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_were_any_of_these_in_the_past_three_months')
        THEN 'REDACTED'
        ELSE spi_were_any_of_these_in_the_past_three_months::text
    END as spi_were_any_of_these_in_the_past_three_months,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_in_the_past_month_have_you_been_thinking_about_how')
        THEN 'REDACTED'
        ELSE spi_in_the_past_month_have_you_been_thinking_about_how::text
    END as spi_in_the_past_month_have_you_been_thinking_about_how,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_in_the_past_month_have_you_had_these_thoughts_and_')
        THEN 'REDACTED'
        ELSE spi_in_the_past_month_have_you_had_these_thoughts_and_::text
    END as spi_in_the_past_month_have_you_had_these_thoughts_and_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_in_the_past_month_have_you_started_to_work_out_or_')
        THEN 'REDACTED'
        ELSE spi_in_the_past_month_have_you_started_to_work_out_or_::text
    END as spi_in_the_past_month_have_you_started_to_work_out_or_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_in_the_past_month_have_you_wished_you_were_dead_or')
        THEN 'REDACTED'
        ELSE spi_in_the_past_month_have_you_wished_you_were_dead_or::text
    END as spi_in_the_past_month_have_you_wished_you_were_dead_or,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_have_you_ever_done_anything_started_to_do_anything')
        THEN 'REDACTED'
        ELSE spi_have_you_ever_done_anything_started_to_do_anything::text
    END as spi_have_you_ever_done_anything_started_to_do_anything,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_in_the_past_month_have_you_actually_had_any_though')
        THEN 'REDACTED'
        ELSE spi_in_the_past_month_have_you_actually_had_any_though::text
    END as spi_in_the_past_month_have_you_actually_had_any_though,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_taskid_spi_subform_2')
        THEN 'REDACTED'
        ELSE spi_taskid_spi_subform_2::text
    END as spi_taskid_spi_subform_2,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_43898926e9d5400ea09562a8c7f8dd03')
        THEN 'REDACTED'
        ELSE spi_43898926e9d5400ea09562a8c7f8dd03::text
    END as spi_43898926e9d5400ea09562a8c7f8dd03,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_644c00f28b07491192d0f05f5e5cc837')
        THEN 'REDACTED'
        ELSE spi_644c00f28b07491192d0f05f5e5cc837::text
    END as spi_644c00f28b07491192d0f05f5e5cc837,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_71dbaac9867f4d7fb6d7e1f59402bbf9')
        THEN 'REDACTED'
        ELSE spi_71dbaac9867f4d7fb6d7e1f59402bbf9::text
    END as spi_71dbaac9867f4d7fb6d7e1f59402bbf9,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_7fc39fba84db490daf4e8f772d91ffda')
        THEN 'REDACTED'
        ELSE spi_7fc39fba84db490daf4e8f772d91ffda::text
    END as spi_7fc39fba84db490daf4e8f772d91ffda,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_a6601f970a3449d0b181611d6705ca42')
        THEN 'REDACTED'
        ELSE spi_a6601f970a3449d0b181611d6705ca42::text
    END as spi_a6601f970a3449d0b181611d6705ca42,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_c7d21a570fc64e3ab411beceb1fb2423')
        THEN 'REDACTED'
        ELSE spi_c7d21a570fc64e3ab411beceb1fb2423::text
    END as spi_c7d21a570fc64e3ab411beceb1fb2423,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_ecabd308c45a41f78754b1f63a101019')
        THEN 'REDACTED'
        ELSE spi_ecabd308c45a41f78754b1f63a101019::text
    END as spi_ecabd308c45a41f78754b1f63a101019,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_taskstatus_spi_subform_2')
        THEN 'REDACTED'
        ELSE spi_taskstatus_spi_subform_2::text
    END as spi_taskstatus_spi_subform_2,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-id-spi-subform-3')
        THEN 'REDACTED'
        ELSE "task-id-spi-subform-3"::text
    END as "task-id-spi-subform-3",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-id-spi-subform-4')
        THEN 'REDACTED'
        ELSE "task-id-spi-subform-4"::text
    END as "task-id-spi-subform-4",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-status-spi-subform-3')
        THEN 'REDACTED'
        ELSE "task-status-spi-subform-3"::text
    END as "task-status-spi-subform-3",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-status-spi-subform-4')
        THEN 'REDACTED'
        ELSE "task-status-spi-subform-4"::text
    END as "task-status-spi-subform-4",
        CURRENT_TIMESTAMP as anonymized_at

from source_data