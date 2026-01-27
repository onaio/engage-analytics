-- ABOUTME: Maps practitioners to organizations via CareTeam membership
-- ABOUTME: CareTeams contain both Practitioner and Organization participants

{{
    config(
        materialized='view'
    )
}}

-- CareTeams link practitioners to organizations through participant list
-- Each CareTeam has participants of type 'Practitioner' and 'Organization'
-- We join these to create the mapping

with careteam_orgs as (
    select
        id as careteam_id,
        participant_id as organization_id,
        _airbyte_emitted_at
    from {{ ref('unnested_careteams') }}
    where participant_type = 'Organization'
),

careteam_practitioners as (
    select
        id as careteam_id,
        participant_id as practitioner_id,
        _airbyte_emitted_at
    from {{ ref('unnested_careteams') }}
    where participant_type = 'Practitioner'
),

-- Join to get practitioner -> organization mapping
-- A practitioner may be in multiple CareTeams (take most recent)
practitioner_org_mapping as (
    select
        cp.practitioner_id,
        co.organization_id,
        cp.careteam_id,
        greatest(cp._airbyte_emitted_at, co._airbyte_emitted_at) as _airbyte_emitted_at,
        row_number() over (
            partition by cp.practitioner_id
            order by greatest(cp._airbyte_emitted_at, co._airbyte_emitted_at) desc
        ) as rn
    from careteam_practitioners cp
    inner join careteam_orgs co on co.careteam_id = cp.careteam_id
)

select
    practitioner_id,
    organization_id,
    careteam_id,
    _airbyte_emitted_at
from practitioner_org_mapping
where rn = 1
