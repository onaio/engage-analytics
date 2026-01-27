-- ABOUTME: Extracts patient data with practitioner/organization assignment
-- ABOUTME: Gets org from most recent encounter for this patient

{{
    config(
        materialized='incremental',
        unique_key='id'
    )
}}

-- Get practitioner/org info from the most recent encounter for each patient
with patient_encounters as (
    select
        subject_id as patient_id,
        practitioner_id,
        practitioner_organization_id,
        practitioner_careteam_id,
        location_id as practitioner_location_id,
        _airbyte_emitted_at,
        row_number() over (
            partition by subject_id
            order by period_start desc nulls last, _airbyte_emitted_at desc
        ) as rn
    from {{ ref('encounters') }}
    where subject_id is not null
)

select
    stp.id,
    stp.gender,
    stp.birthDate::date as birth_date,
    DATE_PART('year', AGE(stp.birthDate::date))::integer as age_years,
    stp.deceasedBoolean as deceased,
    stp.active,
    stp.period_start::date as registration_date,
    pe.practitioner_location_id as location_id,
    stp.name_family,
    TRIM(stp.name_given,'[""]') as name_given,
    stp.telecom_value as phone_number,
    pe.practitioner_id,
    pe.practitioner_organization_id,
    pe.practitioner_careteam_id,
    greatest(
        stp._airbyte_emitted_at,
        coalesce(pe._airbyte_emitted_at, '1900-01-01')
    ) as _airbyte_emitted_at
from {{ ref('stg_patient') }} stp
left join patient_encounters pe
    on pe.patient_id = stp.id
    and pe.rn = 1

{% if is_incremental() %}
    where stp._airbyte_emitted_at > (select max(_airbyte_emitted_at) from {{ this }})
{% endif %}
