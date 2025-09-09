

-- Anonymized view for qr_spi_subform_2 with PII fields masked based on questionnaire_metadata.anon flag
-- Auto-generated to mask fields marked as anon=TRUE in questionnaire_metadata

with metadata_pii as (
    select distinct
        question_alias
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where questionnaire_id in ('Questionnaire/104453')
    and anon = 'TRUE'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_spi_subform_2"
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
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_encounter_id_of_spi_sub_2')
        THEN 'REDACTED'
        ELSE spi_encounter_id_of_spi_sub_2::text
    END as spi_encounter_id_of_spi_sub_2,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_did_this_client_also_screen_positive_for_probable_')
        THEN 'REDACTED'
        ELSE spi_did_this_client_also_screen_positive_for_probable_::text
    END as spi_did_this_client_also_screen_positive_for_probable_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_did_you_discuss_this_clients_severe_mental_health_')
        THEN 'REDACTED'
        ELSE spi_did_you_discuss_this_clients_severe_mental_health_::text
    END as spi_did_you_discuss_this_clients_severe_mental_health_,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_what_is_the_recommended_plan_to_address_the_probab')
        THEN 'REDACTED'
        ELSE spi_what_is_the_recommended_plan_to_address_the_probab::text
    END as spi_what_is_the_recommended_plan_to_address_the_probab,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_what_is_the_recommended_plan_to_address_the_probab_5')
        THEN 'REDACTED'
        ELSE spi_what_is_the_recommended_plan_to_address_the_probab_5::text
    END as spi_what_is_the_recommended_plan_to_address_the_probab_5,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_taskid_spi_subform_3')
        THEN 'REDACTED'
        ELSE spi_taskid_spi_subform_3::text
    END as spi_taskid_spi_subform_3,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_d02eb882411e4bc0af9d08a77fcc3f50')
        THEN 'REDACTED'
        ELSE spi_d02eb882411e4bc0af9d08a77fcc3f50::text
    END as spi_d02eb882411e4bc0af9d08a77fcc3f50,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_taskstatus_spi_subform_3')
        THEN 'REDACTED'
        ELSE spi_taskstatus_spi_subform_3::text
    END as spi_taskstatus_spi_subform_3,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_taskid_spi_subform_4')
        THEN 'REDACTED'
        ELSE spi_taskid_spi_subform_4::text
    END as spi_taskid_spi_subform_4,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_taskstatus_spi_subform_4')
        THEN 'REDACTED'
        ELSE spi_taskstatus_spi_subform_4::text
    END as spi_taskstatus_spi_subform_4,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_53b30f1274eb474c941c5f0e81b7eb9f')
        THEN 'REDACTED'
        ELSE spi_53b30f1274eb474c941c5f0e81b7eb9f::text
    END as spi_53b30f1274eb474c941c5f0e81b7eb9f,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_did_you_discuss_this_clients_suicide_risk_screenin')
        THEN 'REDACTED'
        ELSE spi_did_you_discuss_this_clients_suicide_risk_screenin::text
    END as spi_did_you_discuss_this_clients_suicide_risk_screenin,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_this_client_scored_in_the_moderate_to_high_range_o')
        THEN 'REDACTED'
        ELSE spi_this_client_scored_in_the_moderate_to_high_range_o::text
    END as spi_this_client_scored_in_the_moderate_to_high_range_o,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'spi_783e2646c8ea466dbba885c891a14435')
        THEN 'REDACTED'
        ELSE spi_783e2646c8ea466dbba885c891a14435::text
    END as spi_783e2646c8ea466dbba885c891a14435,
        CURRENT_TIMESTAMP as anonymized_at

from source_data