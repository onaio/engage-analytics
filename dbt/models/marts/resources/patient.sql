{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

with
tags as (
  {{ meta_tag_pivot('stg_encounter') }}
)

select 
    stp.id,
    stp.gender,
    stp.birthDate::date as birth_date,
    stp.deceasedBoolean as deceased,
    stp.active,
    stp.period_start::date as registration_date,
    ut.practitioner_location_id as location_id,
    stp.name_family,
    TRIM(stp.name_given,'[""]') as name_given,
    stp.telecom_value as phone_number,
    ut.practitioner_id,
    ut.practitioner_organization_id,
    ut.practitioner_careteam_id,
    stp._airbyte_emitted_at
from {{ref('stg_patient')}} stp
left join tags ut on ut.resource_id=stp.id

{% if is_incremental() %}
    where stp._airbyte_emitted_at > (select max(_airbyte_emitted_at) from {{ this }})
{% endif %}
