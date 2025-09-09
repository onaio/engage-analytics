
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_gad_7_s3_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_gad_7_s3 with PII fields masked based on questionnaire_metadata.anon flag
-- Auto-generated to mask fields marked as anon=TRUE in questionnaire_metadata

with metadata_pii as (
    select distinct
        question_alias
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where questionnaire_id in ('Questionnaire/59')
    and anon = 'TRUE'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_gad_7_s3"
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
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_q1_anxious')
        THEN 'REDACTED'
        ELSE ipc_s3_q1_anxious::text
    END as ipc_s3_q1_anxious,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_q2_control_worry')
        THEN 'REDACTED'
        ELSE ipc_s3_q2_control_worry::text
    END as ipc_s3_q2_control_worry,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_q3_worry_too_much')
        THEN 'REDACTED'
        ELSE ipc_s3_q3_worry_too_much::text
    END as ipc_s3_q3_worry_too_much,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_q4_trouble_relaxing')
        THEN 'REDACTED'
        ELSE ipc_s3_q4_trouble_relaxing::text
    END as ipc_s3_q4_trouble_relaxing,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_q5_restless')
        THEN 'REDACTED'
        ELSE ipc_s3_q5_restless::text
    END as ipc_s3_q5_restless,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_q6_irritable')
        THEN 'REDACTED'
        ELSE ipc_s3_q6_irritable::text
    END as ipc_s3_q6_irritable,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_q7_afraid')
        THEN 'REDACTED'
        ELSE ipc_s3_q7_afraid::text
    END as ipc_s3_q7_afraid,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_total_score')
        THEN 'REDACTED'
        ELSE ipc_s3_total_score::text
    END as ipc_s3_total_score,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_score_meaning')
        THEN 'REDACTED'
        ELSE ipc_s3_score_meaning::text
    END as ipc_s3_score_meaning,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_score_meaning_10')
        THEN 'REDACTED'
        ELSE ipc_s3_score_meaning_10::text
    END as ipc_s3_score_meaning_10,
        CURRENT_TIMESTAMP as anonymized_at

from source_data
  );