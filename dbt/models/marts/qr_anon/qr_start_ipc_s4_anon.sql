{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_start_ipc_s4
-- Uses questionnaire_metadata.anon column to mask PII fields

{{ create_anonymized_qr_view('qr_start_ipc_s4', []) }}
