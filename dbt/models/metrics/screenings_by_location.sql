-- ABOUTME: Screenings (mwTool) aggregated by practitioner location
-- ABOUTME: Supports indicator #32 (Screening location setting)

{{ config(materialized='view') }}

-- Get mwTool screenings with practitioner info
with screenings as (
    select
        qr.id as screening_id,
        qr.subject_patient_id,
        qr.author_practitioner_id as practitioner_id,
        qr.meta_lastupdated::date as screening_date,
        date_trunc('month', qr.meta_lastupdated::date)::date as screening_month
    from {{ ref('stg_questionnaire_response') }} qr
    where qr.questionnaire_id = 'Questionnaire/1613532'  -- mwTool
      and qr.subject_patient_id is not null
),

-- Join with practitioner to get location
with_location as (
    select
        s.screening_id,
        s.subject_patient_id,
        s.practitioner_id,
        s.screening_date,
        s.screening_month,
        pr.location_id,
        pr.organization_id
    from screenings s
    left join {{ ref('practitioners') }} pr on pr.id = s.practitioner_id
)

select
    screening_id,
    subject_patient_id,
    practitioner_id,
    organization_id,
    location_id,
    screening_date,
    screening_month
from with_location
