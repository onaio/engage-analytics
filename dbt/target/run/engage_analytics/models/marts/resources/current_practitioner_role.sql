
  create view "airbyte"."engage_analytics_engage_analytics_mart"."current_practitioner_role__dbt_tmp"
    
    
  as (
    with ranked as (
  select
    pr.*,
    row_number() over (
      partition by practitioner_id
      order by coalesce(pr.meta_lastupdated::timestamp, pr._airbyte_emitted_at::timestamp) desc
    ) as rn
  from "airbyte"."engage_analytics_engage_analytics_mart"."practitioner_role" pr
  where coalesce(pr.active::text,'true') ilike 'true'
)
select * from ranked where rn = 1
  );