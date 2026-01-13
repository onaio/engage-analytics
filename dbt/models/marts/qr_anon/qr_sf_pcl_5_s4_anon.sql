{{
    config(
        materialized='view'
    )
}}

-- Anonymized view for qr_sf_pcl_5_s4
-- Uses questionnaire_metadata.anon column to mask PII fields

{{ create_anonymized_qr_view('qr_sf_pcl_5_s4', []) }}