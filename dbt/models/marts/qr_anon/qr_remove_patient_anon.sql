{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_remove_patient
-- Uses questionnaire_metadata.anon column to mask PII fields

{{ create_anonymized_qr_view('qr_remove_patient', []) }}