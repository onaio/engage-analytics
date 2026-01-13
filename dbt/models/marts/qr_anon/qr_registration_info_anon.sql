{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_registration_info
-- Uses questionnaire_metadata.anon column to mask PII fields

{{ create_anonymized_qr_view('qr_registration_info', []) }}