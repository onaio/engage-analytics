select
    id as fhir_id,
    resource,
    resource ->> 'id'                            as resource_id,
    resource -> 'name' -> 0 ->> 'family'         as family_name,
    resource -> 'name' -> 0 -> 'given' ->> 0     as given_name,
    resource -> 'telecom' -> 0 ->> 'value'       as phone_raw,
    {{ as_timestamptz("lastUpdated") }}          as lastUpdated_ts,
    _airbyte_extracted_at
from {{ source('airbyte','practitioner') }};
