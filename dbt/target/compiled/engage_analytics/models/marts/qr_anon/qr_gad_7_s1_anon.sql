

-- Anonymized view for qr_gad_7_s1 with PII fields masked based on questionnaire_metadata.anon flag
-- Auto-generated to mask fields marked as anon=TRUE in questionnaire_metadata

with metadata_pii as (
    select distinct
        question_alias
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where questionnaire_id in ('Questionnaire/202')
    and anon = 'TRUE'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_gad_7_s1"
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
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_taskid_gad_pdf')
        THEN 'REDACTED'
        ELSE ipc_s1_taskid_gad_pdf::text
    END as ipc_s1_taskid_gad_pdf,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_encounter_id_of_gad7_session_1')
        THEN 'REDACTED'
        ELSE ipc_s1_encounter_id_of_gad7_session_1::text
    END as ipc_s1_encounter_id_of_gad7_session_1,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_q1_anxious')
        THEN 'REDACTED'
        ELSE ipc_s1_q1_anxious::text
    END as ipc_s1_q1_anxious,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_q2_control_worry')
        THEN 'REDACTED'
        ELSE ipc_s1_q2_control_worry::text
    END as ipc_s1_q2_control_worry,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_q3_worry_too_much')
        THEN 'REDACTED'
        ELSE ipc_s1_q3_worry_too_much::text
    END as ipc_s1_q3_worry_too_much,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_q4_trouble_relaxing')
        THEN 'REDACTED'
        ELSE ipc_s1_q4_trouble_relaxing::text
    END as ipc_s1_q4_trouble_relaxing,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_q5_restless')
        THEN 'REDACTED'
        ELSE ipc_s1_q5_restless::text
    END as ipc_s1_q5_restless,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_q6_irritable')
        THEN 'REDACTED'
        ELSE ipc_s1_q6_irritable::text
    END as ipc_s1_q6_irritable,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_q7_afraid')
        THEN 'REDACTED'
        ELSE ipc_s1_q7_afraid::text
    END as ipc_s1_q7_afraid,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_observation_id_of_gad7_session_1_score')
        THEN 'REDACTED'
        ELSE ipc_s1_observation_id_of_gad7_session_1_score::text
    END as ipc_s1_observation_id_of_gad7_session_1_score,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_total_score')
        THEN 'REDACTED'
        ELSE ipc_s1_total_score::text
    END as ipc_s1_total_score,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_score_meaning')
        THEN 'REDACTED'
        ELSE ipc_s1_score_meaning::text
    END as ipc_s1_score_meaning,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'ipc_s1_score_meaning_13')
        THEN 'REDACTED'
        ELSE ipc_s1_score_meaning_13::text
    END as ipc_s1_score_meaning_13,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'date_of_birth')
        THEN 'REDACTED'
        ELSE date_of_birth::text
    END as date_of_birth,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'age')
        THEN 'REDACTED'
        ELSE age::text
    END as age,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'birth_month')
        THEN 'REDACTED'
        ELSE birth_month::text
    END as birth_month,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'age_years')
        THEN 'REDACTED'
        ELSE age_years::text
    END as age_years,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'encounter_reference')
        THEN 'REDACTED'
        ELSE encounter_reference::text
    END as encounter_reference,
        CURRENT_TIMESTAMP as anonymized_at

from source_data