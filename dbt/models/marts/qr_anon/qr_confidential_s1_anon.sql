{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_confidential_s1 with PII fields masked based on questionnaire_metadata.anon flag
-- Auto-generated to mask fields marked as anon=TRUE in questionnaire_metadata

with metadata_pii as (
    select distinct
        question_alias
    from {{ ref('questionnaire_metadata') }}
    where questionnaire_id in ('Questionnaire/23451')
    and anon = 'TRUE'
),

source_data as (
    select * from {{ ref('qr_confidential_s1') }}
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