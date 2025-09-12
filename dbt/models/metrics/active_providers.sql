-- models/metrics/active_providers.sql
{{
    config(
        materialized='view'
    )
}}

-- View to identify providers who have submitted QRs in the past 30 days
-- This calculates a rolling 30-day window from current_date
with qr_activity as (
  select
    author_practitioner_id as practitioner_id,
    max(meta_lastupdated::date) as last_activity_date,
    count(*) as qr_count_30d
  from {{ ref('stg_questionnaire_response') }}
  where meta_lastupdated::date >= current_date - interval '30 days'
    and author_practitioner_id is not null
  group by 1
)

select
  current_date as period_date,  -- Add period_date for daily tracking
  p.id as practitioner_id,
  p.organization_id,
  p.active,
  qa.last_activity_date,
  qa.qr_count_30d,
  case 
    when qa.practitioner_id is not null then true
    else false
  end as is_active_30d
from {{ ref('practitioners') }} p
left join qr_activity qa on qa.practitioner_id = p.id
where p.active = 'true'