{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_mood_rating_s4
-- Uses questionnaire_metadata.anon column to mask PII fields

{{ create_anonymized_qr_view('qr_mood_rating_s4', []) }}
