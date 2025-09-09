{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_phq_9_s1 with PII fields masked based on questionnaire_metadata.anon flag
-- Auto-generated to mask fields marked as anon=TRUE in questionnaire_metadata

with metadata_pii as (
    select distinct
        question_alias
    from {{ ref('questionnaire_metadata') }}
    where questionnaire_id in ('Questionnaire/54')
    and anon = 'TRUE'
),

source_data as (
    select * from {{ ref('qr_phq_9_s1') }}
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
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'phq9_q1_interest')
        THEN 'REDACTED'
        ELSE phq9_q1_interest::text
    END as phq9_q1_interest,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'phq9_q2_depressed')
        THEN 'REDACTED'
        ELSE phq9_q2_depressed::text
    END as phq9_q2_depressed,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'phq9_q3_sleep')
        THEN 'REDACTED'
        ELSE phq9_q3_sleep::text
    END as phq9_q3_sleep,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'phq9_q4_tired')
        THEN 'REDACTED'
        ELSE phq9_q4_tired::text
    END as phq9_q4_tired,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'phq9_q5_appetite')
        THEN 'REDACTED'
        ELSE phq9_q5_appetite::text
    END as phq9_q5_appetite,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'phq9_q6_failure')
        THEN 'REDACTED'
        ELSE phq9_q6_failure::text
    END as phq9_q6_failure,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'phq9_q7_concentration')
        THEN 'REDACTED'
        ELSE phq9_q7_concentration::text
    END as phq9_q7_concentration,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'phq9_q8_movement')
        THEN 'REDACTED'
        ELSE phq9_q8_movement::text
    END as phq9_q8_movement,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'phq9_q9_suicide')
        THEN 'REDACTED'
        ELSE phq9_q9_suicide::text
    END as phq9_q9_suicide,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'phq9_total_score')
        THEN 'REDACTED'
        ELSE phq9_total_score::text
    END as phq9_total_score,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'phq9_score_meaning')
        THEN 'REDACTED'
        ELSE phq9_score_meaning::text
    END as phq9_score_meaning,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'phq9_score_meaning_code')
        THEN 'REDACTED'
        ELSE phq9_score_meaning_code::text
    END as phq9_score_meaning_code,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'phq9_encounter_id')
        THEN 'REDACTED'
        ELSE phq9_encounter_id::text
    END as phq9_encounter_id,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'phq9_observation_id')
        THEN 'REDACTED'
        ELSE phq9_observation_id::text
    END as phq9_observation_id,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'phq9_calculated_month')
        THEN 'REDACTED'
        ELSE phq9_calculated_month::text
    END as phq9_calculated_month,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'phq9_calculated_year')
        THEN 'REDACTED'
        ELSE phq9_calculated_year::text
    END as phq9_calculated_year,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'phq9_field_cd8e3d6d')
        THEN 'REDACTED'
        ELSE phq9_field_cd8e3d6d::text
    END as phq9_field_cd8e3d6d,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'phq9_task_id')
        THEN 'REDACTED'
        ELSE phq9_task_id::text
    END as phq9_task_id,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'phq9_field_9c366bcc')
        THEN 'REDACTED'
        ELSE phq9_field_9c366bcc::text
    END as phq9_field_9c366bcc,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'phq9_field_b5bc7f80')
        THEN 'REDACTED'
        ELSE phq9_field_b5bc7f80::text
    END as phq9_field_b5bc7f80,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'phq9_link_encounter')
        THEN 'REDACTED'
        ELSE phq9_link_encounter::text
    END as phq9_link_encounter,
        CURRENT_TIMESTAMP as anonymized_at

from source_data