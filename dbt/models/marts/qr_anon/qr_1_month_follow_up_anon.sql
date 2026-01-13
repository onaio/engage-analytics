{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_1_month_follow_up
-- Uses questionnaire_metadata.anon column to mask PII fields

{{ create_anonymized_qr_view('qr_1_month_follow_up', []) }}
