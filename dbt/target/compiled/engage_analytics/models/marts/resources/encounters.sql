

with
tags as (
  with unnested_tags AS (
    select
        id,
        jsonb_array_elements(coalesce(meta::jsonb -> 'tag', '[]'::jsonb)) ->> 'code'    as meta_tag_code,
        jsonb_array_elements(coalesce(meta::jsonb -> 'tag', '[]'::jsonb)) ->> 'display' as meta_tag_display,
        _airbyte_emitted_at
    from "airbyte"."engage_analytics_engage_analytics_stg"."stg_encounter"
),
tags_renamed as (
    select
      id as resource_id,
      _airbyte_emitted_at,
      meta_tag_code,
      case
        when meta_tag_display = 'Practitioner Location'    then 'practitioner_location_id'
        when meta_tag_display = 'Practitioner Organization' then 'practitioner_organization_id'
        when meta_tag_display = 'Practitioner'              then 'practitioner_id'
        when meta_tag_display = 'Practitioner CareTeam'     then 'practitioner_careteam_id'
        when meta_tag_display = 'Application Version'       then 'application_version'
        else null
      end as display_name
    from unnested_tags
)
select
  resource_id,
  _airbyte_emitted_at,
  
  
    max(
      
      case
      when display_name = 'practitioner_location_id'
        then meta_tag_code
      else null
      end
    )
    
      
            as "practitioner_location_id"
      
    
    ,
  
    max(
      
      case
      when display_name = 'practitioner_organization_id'
        then meta_tag_code
      else null
      end
    )
    
      
            as "practitioner_organization_id"
      
    
    ,
  
    max(
      
      case
      when display_name = 'practitioner_id'
        then meta_tag_code
      else null
      end
    )
    
      
            as "practitioner_id"
      
    
    ,
  
    max(
      
      case
      when display_name = 'practitioner_careteam_id'
        then meta_tag_code
      else null
      end
    )
    
      
            as "practitioner_careteam_id"
      
    
    ,
  
    max(
      
      case
      when display_name = 'application_version'
        then meta_tag_code
      else null
      end
    )
    
      
            as "application_version"
      
    
    
  

from tags_renamed

  where _airbyte_emitted_at > (select max(_airbyte_emitted_at) from "airbyte"."engage_analytics_engage_analytics_mart"."encounters")

group by resource_id, _airbyte_emitted_at
),
codes as (
  -- Only json OBJECT columns here (arrays like reasonCode/participant are handled in SELECT)
  select
    id::text as resource_id,
        class::jsonb ->> 'code' as class_code,
        class::jsonb ->> 'system' as class_system,
        priority::jsonb ->> 'coding' as priority_coding,
        priority::jsonb ->> 'text' as priority_text,
        period::jsonb ->> 'start' as period_start,
        period::jsonb ->> 'end' as period_end,
        serviceprovider::jsonb ->> 'reference' as serviceprovider_reference,
    /* use the column your staging models actually expose */
    _airbyte_emitted_at
  from "airbyte"."engage_analytics_engage_analytics_stg"."stg_encounter"
)

select
  e.id,
  e.status,
  -- reasonCode is typically an array; grab the first element’s 'text' safely
  (e.reasoncode::jsonb -> 0 ->> 'text') as reasoncode,
  
  split_part(e.subject::jsonb->>'reference', '/', 1)
 as subject_type,
  
  split_part(e.subject::jsonb->>'reference', '/', 2)
   as subject_id,
  date_trunc('month', (c.period_start)::date) as month,
  -- serviceType coding (jsonb)
  (e.servicetype::jsonb -> 'coding' -> 0 ->> 'code')    as service_coding,
  (e.servicetype::jsonb -> 'coding' -> 0 ->> 'display') as service_display,

  -- provider / class / priority decoded in 'codes' CTE
  c.serviceprovider_reference,
  c.class_code,
  c.priority_text,
  c.period_start,
  c.period_end,

  -- tags (meta->tag)
  t.practitioner_location_id as location_id,
  t.practitioner_organization_id,
  t.practitioner_careteam_id,
  t.practitioner_id,

  -- keep the raw participant json for downstream use (it’s an array)
  e.participant,
  e._airbyte_emitted_at
from "airbyte"."engage_analytics_engage_analytics_stg"."stg_encounter" e
left join tags  t on t.resource_id = e.id
left join codes c on c.resource_id = e.id

where e._airbyte_emitted_at > (
  select coalesce(max(_airbyte_emitted_at), '1900-01-01') from "airbyte"."engage_analytics_engage_analytics_mart"."encounters"
)
