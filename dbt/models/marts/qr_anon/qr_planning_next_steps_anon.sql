{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_planning_next_steps
-- Uses questionnaire_metadata.anon column to mask PII fields

{{ create_anonymized_qr_view('qr_planning_next_steps', []) }}