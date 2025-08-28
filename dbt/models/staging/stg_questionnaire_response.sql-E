with base as (
    select
        id as fhir_id,
        resource,
        resource ->> 'id'                          as resource_id,
        resource ->> 'status'                      as status,
        resource ->> 'questionnaire'               as questionnaire_canonical,
        {{ ref_id_from_reference("resource -> 'subject' ->> 'reference'") }}    as patient_id,
        {{ ref_id_from_reference("resource -> 'encounter' ->> 'reference'") }}  as encounter_id,
        {{ ref_id_from_reference("resource -> 'author' ->> 'reference'") }}     as author_id,
        {{ as_timestamptz("resource ->> 'authored'") }} as authored_ts,
        {{ as_timestamptz("lastUpdated") }}        as lastUpdated_ts,
        _airbyte_extracted_at
    from {{ source('airbyte','questionnaire_response') }}
)
select * from base;
