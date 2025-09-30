{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_phq_9_s1
-- Uses questionnaire_metadata.anon column to mask PII fields

{{ create_anonymized_qr_view('qr_phq_9_s1', []) }}
