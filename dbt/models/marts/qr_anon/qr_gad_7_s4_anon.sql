{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_gad_7_s4
-- Automatically generated based on questionnaire_metadata.csv

select 
    questionnaire_id as questionnaire_id,
    subject_patient_id as subject_patient_id,
    encounter_id as encounter_id,
    author_practitioner_id as author_practitioner_id,
    practitioner_location_id as practitioner_location_id,
    practitioner_organization_id as practitioner_organization_id,
    practitioner_id as practitioner_id,
    practitioner_careteam_id as practitioner_careteam_id,
    application_version as application_version,
    qr_id as qr_id,
    gad_place_declare_values as gad_place_declare_values,
    gad_place_declare_values_2 as gad_place_declare_values_2,
    gad_place_declare_values_3 as gad_place_declare_values_3,
    feeling_nervous as feeling_nervous,
    gad_gad_ipc_session as gad_gad_ipc_session,
    excessive_worry as excessive_worry,
    trouble_relaxing as trouble_relaxing,
    restlessness as restlessness,
    irritability as irritability,
    feeling_afraid as feeling_afraid,
    CURRENT_TIMESTAMP as anonymized_at

from {{ ref('qr_gad_7_s4') }}