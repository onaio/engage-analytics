{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_gad_7_s2 with PII fields masked based on questionnaire_metadata.anon flag
-- Auto-generated to mask fields marked as anon=TRUE in questionnaire_metadata

with metadata_pii as (
    select distinct
        question_alias
    from {{ ref('questionnaire_metadata') }}
    where questionnaire_id in ('Questionnaire/205')
    and anon = 'TRUE'
),

source_data as (
    select * from {{ ref('qr_gad_7_s2') }}
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
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_encounter_id_of_gad7_session_2')
        THEN 'REDACTED'
        ELSE ipc_s2_encounter_id_of_gad7_session_2::text
    END as ipc_s2_encounter_id_of_gad7_session_2,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_q1_anxious')
        THEN 'REDACTED'
        ELSE ipc_s2_q1_anxious::text
    END as ipc_s2_q1_anxious,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_q2_control_worry')
        THEN 'REDACTED'
        ELSE ipc_s2_q2_control_worry::text
    END as ipc_s2_q2_control_worry,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_q3_worry_too_much')
        THEN 'REDACTED'
        ELSE ipc_s2_q3_worry_too_much::text
    END as ipc_s2_q3_worry_too_much,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_q4_trouble_relaxing')
        THEN 'REDACTED'
        ELSE ipc_s2_q4_trouble_relaxing::text
    END as ipc_s2_q4_trouble_relaxing,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_q5_restless')
        THEN 'REDACTED'
        ELSE ipc_s2_q5_restless::text
    END as ipc_s2_q5_restless,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_q6_irritable')
        THEN 'REDACTED'
        ELSE ipc_s2_q6_irritable::text
    END as ipc_s2_q6_irritable,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_q7_afraid')
        THEN 'REDACTED'
        ELSE ipc_s2_q7_afraid::text
    END as ipc_s2_q7_afraid,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_observation_id_of_gad7_session_2_score')
        THEN 'REDACTED'
        ELSE ipc_s2_observation_id_of_gad7_session_2_score::text
    END as ipc_s2_observation_id_of_gad7_session_2_score,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_total_score')
        THEN 'REDACTED'
        ELSE ipc_s2_total_score::text
    END as ipc_s2_total_score,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_score_meaning')
        THEN 'REDACTED'
        ELSE ipc_s2_score_meaning::text
    END as ipc_s2_score_meaning,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s2_score_meaning_12')
        THEN 'REDACTED'
        ELSE ipc_s2_score_meaning_12::text
    END as ipc_s2_score_meaning_12,
        CURRENT_TIMESTAMP as anonymized_at

from source_data