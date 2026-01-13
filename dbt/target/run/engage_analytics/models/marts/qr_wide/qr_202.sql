
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_202__dbt_tmp"
    
    
  as (
    -- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"

-- GAD-7 (IPC Session 1) (id: ipc-202, url: n/a, version: 0.0.1)




  
  

  
  

  
  

  
  
    
  

  
  

  with ids(ident) as ( values('Questionnaire/202') ),
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
      when linkid = 'f1.19.1'
        then answer_value_text
      else null
      end
    )
    
      
            as "f1.19.1"
      
    
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
      when linkid = 'observation-gad-score-id'
        then answer_value_text
      else null
      end
    )
    
      
            as "observation-gad-score-id"
      
    
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
      when linkid = '69f16284-b8a8-4a24-ab79-03bde5bcfbe9'
        then answer_value_text
      else null
      end
    )
    
      
            as "69f16284-b8a8-4a24-ab79-03bde5bcfbe9"
      
    
    ,
  
    max(
      
      case
      when linkid = 'f1.18.1'
        then answer_value_text
      else null
      end
    )
    
      
            as "f1.18.1"
      
    
    ,
  
    max(
      
      case
      when linkid = 'encounter-id'
        then answer_value_text
      else null
      end
    )
    
      
            as "encounter-id"
      
    
    ,
  
    max(
      
      case
      when linkid = 'f1.21.1'
        then answer_value_text
      else null
      end
    )
    
      
            as "f1.21.1"
      
    
    ,
  
    max(
      
      case
      when linkid = 'LINK_ID_THAT_CONTAINS_ENCOUNTER_ID_AS_ANSWER'
        then answer_value_text
      else null
      end
    )
    
      
            as "LINK_ID_THAT_CONTAINS_ENCOUNTER_ID_AS_ANSWER"
      
    
    ,
  
    max(
      
      case
      when linkid = '829e6d32-3fa7-4d16-909b-b198aea54603'
        then answer_value_text
      else null
      end
    )
    
      
            as "829e6d32-3fa7-4d16-909b-b198aea54603"
      
    
    ,
  
    max(
      
      case
      when linkid = 'f1.20.1'
        then answer_value_text
      else null
      end
    )
    
      
            as "f1.20.1"
      
    
    ,
  
    max(
      
      case
      when linkid = '3212b2b2-f42f-46e9-b6bd-42fc29671da9'
        then answer_value_text
      else null
      end
    )
    
      
            as "3212b2b2-f42f-46e9-b6bd-42fc29671da9"
      
    
    ,
  
    max(
      
      case
      when linkid = 'f1.17.1'
        then answer_value_text
      else null
      end
    )
    
      
            as "f1.17.1"
      
    
    ,
  
    max(
      
      case
      when linkid = 'f1.16.1'
        then answer_value_text
      else null
      end
    )
    
      
            as "f1.16.1"
      
    
    ,
  
    max(
      
      case
      when linkid = '583c5b02-b26c-4821-8da8-7ed9066d9d5d'
        then answer_value_text
      else null
      end
    )
    
      
            as "583c5b02-b26c-4821-8da8-7ed9066d9d5d"
      
    
    ,
  
    max(
      
      case
      when linkid = 'f1.15.1'
        then answer_value_text
      else null
      end
    )
    
      
            as "f1.15.1"
      
    
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

  
  


  );