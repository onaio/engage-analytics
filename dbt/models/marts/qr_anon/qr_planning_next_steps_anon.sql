{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_planning_next_steps with PII fields masked based on questionnaire_metadata.anon flag
-- Auto-generated to mask fields marked as anon=TRUE in questionnaire_metadata

with metadata_pii as (
    select distinct
        question_alias
    from {{ ref('questionnaire_metadata') }}
    where questionnaire_id in ('Questionnaire/q-planning-next-steps')
    and anon = 'TRUE'
),

source_data as (
    select * from {{ ref('qr_planning_next_steps') }}
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
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'referral_to_mental_health_specialist')
        THEN 'REDACTED'
        ELSE referral_to_mental_health_specialist::text
    END as referral_to_mental_health_specialist,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'referred')
        THEN 'REDACTED'
        ELSE referred::text
    END as referred,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'okay')
        THEN 'REDACTED'
        ELSE okay::text
    END as okay,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'field_2cb66889')
        THEN 'REDACTED'
        ELSE field_2cb66889::text
    END as field_2cb66889,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'field_34a56c28')
        THEN 'REDACTED'
        ELSE field_34a56c28::text
    END as field_34a56c28,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'referres')
        THEN 'REDACTED'
        ELSE referres::text
    END as referres,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'field_34ab6c28')
        THEN 'REDACTED'
        ELSE field_34ab6c28::text
    END as field_34ab6c28,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'field_3b9f7e5d')
        THEN 'REDACTED'
        ELSE field_3b9f7e5d::text
    END as field_3b9f7e5d,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_motivational_interviewing')
        THEN 'REDACTED'
        ELSE sbirt_motivational_interviewing::text
    END as sbirt_motivational_interviewing,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'safety')
        THEN 'REDACTED'
        ELSE safety::text
    END as safety,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'others')
        THEN 'REDACTED'
        ELSE others::text
    END as others,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'mental_health')
        THEN 'REDACTED'
        ELSE mental_health::text
    END as mental_health,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'not_interested')
        THEN 'REDACTED'
        ELSE not_interested::text
    END as not_interested,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = '20250924')
        THEN 'REDACTED'
        ELSE "20250924"::text
    END as "20250924",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'field_69712544')
        THEN 'REDACTED'
        ELSE field_69712544::text
    END as field_69712544,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'field_76ba8dbe')
        THEN 'REDACTED'
        ELSE field_76ba8dbe::text
    END as field_76ba8dbe,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'hh')
        THEN 'REDACTED'
        ELSE hh::text
    END as hh,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = '20260505')
        THEN 'REDACTED'
        ELSE "20260505"::text
    END as "20260505",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'field_9a8b7c6d')
        THEN 'REDACTED'
        ELSE field_9a8b7c6d::text
    END as field_9a8b7c6d,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'sbirt_motivational_interviewingbrspan_stylecolor006')
        THEN 'REDACTED'
        ELSE sbirt_motivational_interviewingbrspan_stylecolor006::text
    END as sbirt_motivational_interviewingbrspan_stylecolor006,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'field_ab12d39f')
        THEN 'REDACTED'
        ELSE field_ab12d39f::text
    END as field_ab12d39f,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'they_gtycyvuy')
        THEN 'REDACTED'
        ELSE they_gtycyvuy::text
    END as they_gtycyvuy,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'field_b2c3d4e5')
        THEN 'REDACTED'
        ELSE field_b2c3d4e5::text
    END as field_b2c3d4e5,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'test')
        THEN 'REDACTED'
        ELSE test::text
    END as test,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'field_bc2def4a')
        THEN 'REDACTED'
        ELSE field_bc2def4a::text
    END as field_bc2def4a,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'field_c3d4e5f6')
        THEN 'REDACTED'
        ELSE field_c3d4e5f6::text
    END as field_c3d4e5f6,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'field_c9d8b7a6')
        THEN 'REDACTED'
        ELSE field_c9d8b7a6::text
    END as field_c9d8b7a6,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'not_interested_28')
        THEN 'REDACTED'
        ELSE not_interested_28::text
    END as not_interested_28,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'field_cd34f5b1')
        THEN 'REDACTED'
        ELSE field_cd34f5b1::text
    END as field_cd34f5b1,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'iwbwb')
        THEN 'REDACTED'
        ELSE iwbwb::text
    END as iwbwb,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'needs_checkups')
        THEN 'REDACTED'
        ELSE needs_checkups::text
    END as needs_checkups,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'hhh')
        THEN 'REDACTED'
        ELSE hhh::text
    END as hhh,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = '20250827')
        THEN 'REDACTED'
        ELSE "20250827"::text
    END as "20250827",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'referral_to_mental_health_specialist_34')
        THEN 'REDACTED'
        ELSE referral_to_mental_health_specialist_34::text
    END as referral_to_mental_health_specialist_34,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'already_received_a_similar_service')
        THEN 'REDACTED'
        ELSE already_received_a_similar_service::text
    END as already_received_a_similar_service,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'field_ef5617d3')
        THEN 'REDACTED'
        ELSE field_ef5617d3::text
    END as field_ef5617d3,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'peace')
        THEN 'REDACTED'
        ELSE peace::text
    END as peace,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'interested_but_not_now')
        THEN 'REDACTED'
        ELSE interested_but_not_now::text
    END as interested_but_not_now,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'supervisors_name')
        THEN 'REDACTED'
        ELSE supervisors_name::text
    END as supervisors_name,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'd4e5f6a7-b8c9-4d0e-1f2a-3b4c5d6e7f8g')
        THEN 'REDACTED'
        ELSE "d4e5f6a7-b8c9-4d0e-1f2a-3b4c5d6e7f8g"::text
    END as "d4e5f6a7-b8c9-4d0e-1f2a-3b4c5d6e7f8g",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'e5f6a7b8-c9d0-4e1f-2a3b-4c5d6e7f8g9h')
        THEN 'REDACTED'
        ELSE "e5f6a7b8-c9d0-4e1f-2a3b-4c5d6e7f8g9h"::text
    END as "e5f6a7b8-c9d0-4e1f-2a3b-4c5d6e7f8g9h",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'f6a7b8c9-d0e1-4f2a-3b4c-5d6e7f8g9h0i')
        THEN 'REDACTED'
        ELSE "f6a7b8c9-d0e1-4f2a-3b4c-5d6e7f8g9h0i"::text
    END as "f6a7b8c9-d0e1-4f2a-3b4c-5d6e7f8g9h0i",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'financial-hardships-list')
        THEN 'REDACTED'
        ELSE "financial-hardships-list"::text
    END as "financial-hardships-list",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'is-fws')
        THEN 'REDACTED'
        ELSE "is-fws"::text
    END as "is-fws",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'is-ipc')
        THEN 'REDACTED'
        ELSE "is-ipc"::text
    END as "is-ipc",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'is-ipc-main')
        THEN 'REDACTED'
        ELSE "is-ipc-main"::text
    END as "is-ipc-main",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'is-ipc-optional')
        THEN 'REDACTED'
        ELSE "is-ipc-optional"::text
    END as "is-ipc-optional",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'is-mhs')
        THEN 'REDACTED'
        ELSE "is-mhs"::text
    END as "is-mhs",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'is-other-agency-services')
        THEN 'REDACTED'
        ELSE "is-other-agency-services"::text
    END as "is-other-agency-services",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'is-other-services-outside-agency')
        THEN 'REDACTED'
        ELSE "is-other-services-outside-agency"::text
    END as "is-other-services-outside-agency",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'is-sbirt')
        THEN 'REDACTED'
        ELSE "is-sbirt"::text
    END as "is-sbirt",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'is-sbirt-main')
        THEN 'REDACTED'
        ELSE "is-sbirt-main"::text
    END as "is-sbirt-main",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'is-sbirt-optional')
        THEN 'REDACTED'
        ELSE "is-sbirt-optional"::text
    END as "is-sbirt-optional",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'is-spi')
        THEN 'REDACTED'
        ELSE "is-spi"::text
    END as "is-spi",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'patient-age')
        THEN 'REDACTED'
        ELSE "patient-age"::text
    END as "patient-age",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'patient-dob')
        THEN 'REDACTED'
        ELSE "patient-dob"::text
    END as "patient-dob",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'patient-name')
        THEN 'REDACTED'
        ELSE "patient-name"::text
    END as "patient-name",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'scheduled-ipc-session-1-with-current-time')
        THEN 'REDACTED'
        ELSE "scheduled-ipc-session-1-with-current-time"::text
    END as "scheduled-ipc-session-1-with-current-time",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'scheduled-ipc-session-1-with-current-time-2')
        THEN 'REDACTED'
        ELSE "scheduled-ipc-session-1-with-current-time-2"::text
    END as "scheduled-ipc-session-1-with-current-time-2",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-id-000')
        THEN 'REDACTED'
        ELSE "task-id-000"::text
    END as "task-id-000",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-id-001')
        THEN 'REDACTED'
        ELSE "task-id-001"::text
    END as "task-id-001",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-id-002')
        THEN 'REDACTED'
        ELSE "task-id-002"::text
    END as "task-id-002",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-id-003')
        THEN 'REDACTED'
        ELSE "task-id-003"::text
    END as "task-id-003",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-id-004')
        THEN 'REDACTED'
        ELSE "task-id-004"::text
    END as "task-id-004",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-id-006')
        THEN 'REDACTED'
        ELSE "task-id-006"::text
    END as "task-id-006",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-id-planning-next-steps-pdf')
        THEN 'REDACTED'
        ELSE "task-id-planning-next-steps-pdf"::text
    END as "task-id-planning-next-steps-pdf",
        CURRENT_TIMESTAMP as anonymized_at

from source_data