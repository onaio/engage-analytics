
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_spi_subform_3_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_spi_subform_3
-- Uses questionnaire_metadata.anon column to mask PII fields



with metadata_pii as (
    select distinct
        "column",
        linkid,
        short_name,
        anon
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where "table" = 'qr_spi_subform_3'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_spi_subform_3"
)

select
    

    -- Hash patient and QR IDs for privacy
    MD5(qr_id::text) as qr_id_hash,
    questionnaire_id,
    MD5(COALESCE(subject_patient_id, '')::text) as subject_patient_id_hash,
    encounter_id,
    author_practitioner_id,
    practitioner_location_id,
    practitioner_organization_id,
    practitioner_id,
    practitioner_careteam_id,
    application_version,

    -- Process all other columns dynamically
    
        
    
        
    
        
    
        
    
        
    
        
    
        
    
        
    
        
    
        
    
        
            -- Check if this column should be masked based on metadata
            -- Match using same logic as non-anon views: COALESCE(column, linkid)
            CASE
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii
                    WHERE ("column" = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_67' OR linkid = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_67' OR short_name = 'a_place_to_declare_values_that_cannot_be_created_using_fhirp_67')
                    AND anon = 'TRUE'
                )
                THEN
                    
                        'REDACTED'
                    
                ELSE "a_place_to_declare_values_that_cannot_be_created_using_fhirp_67"
            END as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_67",
        
    

    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data


  );