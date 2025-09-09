

-- Anonymized view for qr_phq_9_s3 with PII fields masked based on questionnaire_metadata.anon flag
-- Auto-generated to mask fields marked as anon=TRUE in questionnaire_metadata

with metadata_pii as (
    select distinct
        question_alias
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where questionnaire_id in ('Questionnaire/61')
    and anon = 'TRUE'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_phq_9_s3"
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
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_total_score')
        THEN 'REDACTED'
        ELSE ipc_s3_total_score::text
    END as ipc_s3_total_score,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_q8_movement')
        THEN 'REDACTED'
        ELSE ipc_s3_q8_movement::text
    END as ipc_s3_q8_movement,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_q9_suicide')
        THEN 'REDACTED'
        ELSE ipc_s3_q9_suicide::text
    END as ipc_s3_q9_suicide,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_q1_interest')
        THEN 'REDACTED'
        ELSE ipc_s3_q1_interest::text
    END as ipc_s3_q1_interest,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_q2_depressed')
        THEN 'REDACTED'
        ELSE ipc_s3_q2_depressed::text
    END as ipc_s3_q2_depressed,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_q3_sleep')
        THEN 'REDACTED'
        ELSE ipc_s3_q3_sleep::text
    END as ipc_s3_q3_sleep,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_q4_tired')
        THEN 'REDACTED'
        ELSE ipc_s3_q4_tired::text
    END as ipc_s3_q4_tired,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_q5_appetite')
        THEN 'REDACTED'
        ELSE ipc_s3_q5_appetite::text
    END as ipc_s3_q5_appetite,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_q6_failure')
        THEN 'REDACTED'
        ELSE ipc_s3_q6_failure::text
    END as ipc_s3_q6_failure,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_q7_concentration')
        THEN 'REDACTED'
        ELSE ipc_s3_q7_concentration::text
    END as ipc_s3_q7_concentration,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_score_meaning')
        THEN 'REDACTED'
        ELSE ipc_s3_score_meaning::text
    END as ipc_s3_score_meaning,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s3_score_meaning_12')
        THEN 'REDACTED'
        ELSE ipc_s3_score_meaning_12::text
    END as ipc_s3_score_meaning_12,
        CURRENT_TIMESTAMP as anonymized_at

from source_data