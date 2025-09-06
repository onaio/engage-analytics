with ranked as (
  select
    pr.*,
    row_number() over (
      partition by practitioner_id
      order by coalesce(pr.meta_lastupdated::timestamp, pr._airbyte_emitted_at::timestamp) desc
    ) as rn
  from {{ ref('practitioner_role') }} pr
  where coalesce(pr.active::text,'true') ilike 'true'
)
select * from ranked where rn = 1
