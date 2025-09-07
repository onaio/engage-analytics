




select
    resource::jsonb ->> 'id' as id,
    resource::jsonb ->> 'meta' as meta,
    resource::jsonb ->> 'gender' as gender,
    resource::jsonb ->> 'resourceType' as resourceType,
    resource::jsonb ->> 'birthDate' as birthDate,
    resource::jsonb ->> 'managingOrganization' as managingOrganization,
    resource::jsonb ->> 'deceasedBoolean' as deceasedBoolean,
    resource::jsonb ->> 'active' as active,
    resource::jsonb ->> 'text' as text,
    resource::jsonb ->> 'identifier' as identifier,
((resource::jsonb -> 'telecom')::jsonb->>0)::jsonb ->> 'value'  as telecom_value,
((resource::jsonb -> 'generalPractitioner')::jsonb->>0)::jsonb ->> 'reference' as generalPractitioner_reference,
((resource::jsonb -> 'name')::jsonb->>0)::jsonb ->> 'given' as name_given,
((resource::jsonb -> 'name')::jsonb->>0)::jsonb ->> 'family' as name_family,
((resource::jsonb -> 'identifier')::jsonb->>0)::jsonb -> 'period' ->> 'start' as period_start,
_airbyte_extracted_at as _airbyte_emitted_at
from "airbyte"."airbyte"."patient"


  where _airbyte_extracted_at > (select max(_airbyte_emitted_at) from "airbyte"."engage_analytics_engage_analytics_stg"."stg_patient")
