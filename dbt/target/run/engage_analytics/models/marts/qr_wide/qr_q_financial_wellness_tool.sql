
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_q_financial_wellness_tool__dbt_tmp"
    
    
  as (
    -- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"

-- Financial Wellness Tool (id: q-financial-wellness-tool, url: n/a, version: 0.0.1)




  
  

  
  

  
  

  
  
    
  

  
  

  with ids(ident) as ( values('Questionnaire/q-financial-wellness-tool') ),
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
      when linkid = 'moderate-housing-insecurity'
        then answer_value_text
      else null
      end
    )
    
      
            as "moderate-housing-insecurity"
      
    
    ,
  
    max(
      
      case
      when linkid = 'overall-financial-hardship'
        then answer_value_text
      else null
      end
    )
    
      
            as "overall-financial-hardship"
      
    
    ,
  
    max(
      
      case
      when linkid = 'moderate-medical-hardships'
        then answer_value_text
      else null
      end
    )
    
      
            as "moderate-medical-hardships"
      
    
    ,
  
    max(
      
      case
      when linkid = 'cdef0123-4567-89ab-cdef-0123456789ab'
        then answer_value_text
      else null
      end
    )
    
      
            as "cdef0123-4567-89ab-cdef-0123456789ab"
      
    
    ,
  
    max(
      
      case
      when linkid = '51234567-89ab-cdef-0123-456789abcdef'
        then answer_value_text
      else null
      end
    )
    
      
            as "51234567-89ab-cdef-0123-456789abcdef"
      
    
    ,
  
    max(
      
      case
      when linkid = '31234567-89ab-cdef-0123-456789abcdef'
        then answer_value_text
      else null
      end
    )
    
      
            as "31234567-89ab-cdef-0123-456789abcdef"
      
    
    ,
  
    max(
      
      case
      when linkid = '91234567-89ab-cdef-0123-456789abcdef'
        then answer_value_text
      else null
      end
    )
    
      
            as "91234567-89ab-cdef-0123-456789abcdef"
      
    
    ,
  
    max(
      
      case
      when linkid = 'high-housing-insecurity'
        then answer_value_text
      else null
      end
    )
    
      
            as "high-housing-insecurity"
      
    
    ,
  
    max(
      
      case
      when linkid = 'high-food-insecurity'
        then answer_value_text
      else null
      end
    )
    
      
            as "high-food-insecurity"
      
    
    ,
  
    max(
      
      case
      when linkid = 'high-debt'
        then answer_value_text
      else null
      end
    )
    
      
            as "high-debt"
      
    
    ,
  
    max(
      
      case
      when linkid = 'f1a2b3c4-5e6f-4a1b-8c7d-9e0f1a2b3c4d'
        then answer_value_text
      else null
      end
    )
    
      
            as "f1a2b3c4-5e6f-4a1b-8c7d-9e0f1a2b3c4d"
      
    
    ,
  
    max(
      
      case
      when linkid = 'f1234567-89ab-cdef-0123-456789abcdef'
        then answer_value_text
      else null
      end
    )
    
      
            as "f1234567-89ab-cdef-0123-456789abcdef"
      
    
    ,
  
    max(
      
      case
      when linkid = 'c1234567-89ab-cdef-0123-456789abcdef'
        then answer_value_text
      else null
      end
    )
    
      
            as "c1234567-89ab-cdef-0123-456789abcdef"
      
    
    ,
  
    max(
      
      case
      when linkid = 'high-paying-bills'
        then answer_value_text
      else null
      end
    )
    
      
            as "high-paying-bills"
      
    
    ,
  
    max(
      
      case
      when linkid = '3456789a-bcde-f012-3456-789abcdef012'
        then answer_value_text
      else null
      end
    )
    
      
            as "3456789a-bcde-f012-3456-789abcdef012"
      
    
    ,
  
    max(
      
      case
      when linkid = 'moderate-debt'
        then answer_value_text
      else null
      end
    )
    
      
            as "moderate-debt"
      
    
    ,
  
    max(
      
      case
      when linkid = 'moderate-food-insecurity'
        then answer_value_text
      else null
      end
    )
    
      
            as "moderate-food-insecurity"
      
    
    ,
  
    max(
      
      case
      when linkid = '61234567-89ab-cdef-0123-456789abcdef'
        then answer_value_text
      else null
      end
    )
    
      
            as "61234567-89ab-cdef-0123-456789abcdef"
      
    
    ,
  
    max(
      
      case
      when linkid = '6789abcd-ef01-2345-6789-abcdef012345'
        then answer_value_text
      else null
      end
    )
    
      
            as "6789abcd-ef01-2345-6789-abcdef012345"
      
    
    ,
  
    max(
      
      case
      when linkid = '21234567-89ab-cdef-0123-456789abcdef'
        then answer_value_text
      else null
      end
    )
    
      
            as "21234567-89ab-cdef-0123-456789abcdef"
      
    
    ,
  
    max(
      
      case
      when linkid = 'no-financial-hardship'
        then answer_value_text
      else null
      end
    )
    
      
            as "no-financial-hardship"
      
    
    ,
  
    max(
      
      case
      when linkid = 'moderate-paying-bills'
        then answer_value_text
      else null
      end
    )
    
      
            as "moderate-paying-bills"
      
    
    ,
  
    max(
      
      case
      when linkid = 'f0123456-789a-bcde-f012-3456789abcde'
        then answer_value_text
      else null
      end
    )
    
      
            as "f0123456-789a-bcde-f012-3456789abcde"
      
    
    
  

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