{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_sociodemographic_survey
-- Uses questionnaire_metadata.anon column to mask PII fields

{{ create_anonymized_qr_view('qr_sociodemographic_survey', []) }}