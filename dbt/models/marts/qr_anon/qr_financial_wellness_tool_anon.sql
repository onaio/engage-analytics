{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_financial_wellness_tool
-- Uses questionnaire_metadata.anon column to mask PII fields

{{ create_anonymized_qr_view('qr_financial_wellness_tool', []) }}
