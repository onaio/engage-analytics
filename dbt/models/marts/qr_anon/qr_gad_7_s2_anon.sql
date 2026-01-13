{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_gad_7_s2
-- Uses questionnaire_metadata.anon column to mask PII fields

{{ create_anonymized_qr_view('qr_gad_7_s2', []) }}