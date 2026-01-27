-- ABOUTME: Staging model for FHIR Observation resources
-- ABOUTME: Contains pre-computed assessment scores (PHQ-9, GAD-7, Mood, PTSD)

{{ config(materialized='view', schema='engage_analytics_stg') }}

select
    id as observation_id,
    resource->>'resourceType' as resource_type,
    resource->>'status' as status,
    resource->'code'->'coding'->0->>'code' as observation_code,
    (resource->>'effectiveDateTime')::timestamp as effective_date,
    replace(resource->'subject'->>'reference', 'Patient/', '') as subject_patient_id,
    replace(resource->'performer'->0->>'reference', 'Practitioner/', '') as practitioner_id,
    replace(resource->'derivedFrom'->0->>'reference', 'QuestionnaireResponse/', '') as questionnaire_response_id,
    -- Extract organization from tags
    (select t->>'code' from jsonb_array_elements(resource->'meta'->'tag') t
     where t->>'system' = 'https://smartregister.org/organisation-tag-id' limit 1) as organization_id,
    -- Value would be here if present
    resource->'valueQuantity'->>'value' as value_quantity,
    resource->'valueInteger' as value_integer,
    resource->'valueString' as value_string,
    _airbyte_extracted_at
from {{ source('engage_dataset', 'observation') }}
