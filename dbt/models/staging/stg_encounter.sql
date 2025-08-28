select
    id as fhir_id,
    resource,
    resource ->> 'id'                                 as resource_id,
    resource ->> 'status'                             as status,
    resource -> 'class' ->> 'code'                    as class_code,
    resource -> 'type'  -> 0 -> 'coding' -> 0 ->> 'code' as type_code,
    resource -> 'type'  -> 0 -> 'coding' -> 0 ->> 'display' as type_display,
    {{ ref_id_from_reference("resource -> 'subject' ->> 'reference'") }} as patient_id,
    {{ ref_id_from_reference("resource -> 'participant' -> 0 -> 'individual' ->> 'reference'") }} as practitioner_id,
    {{ as_timestamptz("resource ->> 'period' ->> 'start'") }} as period_start,
    {{ as_timestamptz("resource ->> 'period' ->> 'end'") }}   as period_end,
    {{ as_timestamptz("lastUpdated") }}                       as lastUpdated_ts,
    _airbyte_extracted_at
from {{ source('airbyte','encounter') }}
