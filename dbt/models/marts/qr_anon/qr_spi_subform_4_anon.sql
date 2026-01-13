{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_spi_subform_4
-- Uses questionnaire_metadata.anon column to mask PII fields

{{ create_anonymized_qr_view('qr_spi_subform_4', []) }}
