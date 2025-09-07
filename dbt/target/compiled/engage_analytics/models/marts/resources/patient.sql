

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

  where _airbyte_emitted_at > (select max(_airbyte_emitted_at) from "airbyte"."engage_analytics_engage_analytics_mart"."patient")

group by resource_id, _airbyte_emitted_at
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
from "airbyte"."engage_analytics_engage_analytics_stg"."stg_patient" stp
left join tags ut on ut.resource_id=stp.id


    where stp._airbyte_emitted_at > (select max(_airbyte_emitted_at) from "airbyte"."engage_analytics_engage_analytics_mart"."patient")
