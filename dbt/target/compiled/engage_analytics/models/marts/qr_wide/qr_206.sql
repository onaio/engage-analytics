-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"

-- Mood Rating (IPC Session 2) (id: ipc-206, url: n/a, version: 0.0.1)




  
  

  
  

  
  

  
  
    
  

  
  

  with ids(ident) as ( values('Questionnaire/206') ),
  base as (
    select
      a.qr_id,
      a.linkid,
      string_agg(distinct a.answer_value_text, ' | ' order by a.answer_value_text) as answer_value_text
    from "airbyte"."engage_analytics_engage_analytics_int"."int_qr_answers_long" a
    join ids on a.questionnaire_id = ids.ident
    group by 1,2
  ),
  header as (
    select h.*
    from "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header" h
    join ids on h.questionnaire_id = ids.ident
  ),
  tags as (
    select * from "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
  ),
  pivoted as (
    select
      b.qr_id,
      
  
    max(
      
      case
      when linkid = 'cd8e3d6d-e9ff-458d-d122-57070bebffaf'
        then answer_value_text
      else null
      end
    )
    
      
            as "cd8e3d6d-e9ff-458d-d122-57070bebffaf"
      
    
    ,
  
    max(
      
      case
      when linkid = 'calculated-month'
        then answer_value_text
      else null
      end
    )
    
      
            as "calculated-month"
      
    
    ,
  
    max(
      
      case
      when linkid = 'calculated-year'
        then answer_value_text
      else null
      end
    )
    
      
            as "calculated-year"
      
    
    ,
  
    max(
      
      case
      when linkid = '85a3f14c-e4d8-4c45-9088-967ec4ea0d07'
        then answer_value_text
      else null
      end
    )
    
      
            as "85a3f14c-e4d8-4c45-9088-967ec4ea0d07"
      
    
    ,
  
    max(
      
      case
      when linkid = 'f1.33.6'
        then answer_value_text
      else null
      end
    )
    
      
            as "f1.33.6"
      
    
    ,
  
    max(
      
      case
      when linkid = 'b5bc7f80-4a0c-486c-e5eb-32c750036f94'
        then answer_value_text
      else null
      end
    )
    
      
            as "b5bc7f80-4a0c-486c-e5eb-32c750036f94"
      
    
    
  

    from base b
    group by b.qr_id
  )
  select
    h.questionnaire_id,
    h.subject_patient_id,
    h.encounter_id,
    h.author_practitioner_id,
    t.practitioner_location_id,
    t.practitioner_organization_id,
    t.practitioner_id,
    t.practitioner_careteam_id,
    t.application_version,
    p.*
  from pivoted p
  join header h using (qr_id)
  left join tags t on t.resource_id = h.qr_id

  
  

