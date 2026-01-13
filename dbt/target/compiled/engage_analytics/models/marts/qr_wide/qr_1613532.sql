-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"

-- Mental Wellness Tool (id: 1613532, url: n/a, version: 0.0.1)




  
  

  
  

  
  

  
  
    
  

  
  

  with ids(ident) as ( values('Questionnaire/1613532') ),
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
      when linkid = 'q1-to-q3-negative'
        then answer_value_text
      else null
      end
    )
    
      
            as "q1-to-q3-negative"
      
    
    ,
  
    max(
      
      case
      when linkid = 'suicide-risk'
        then answer_value_text
      else null
      end
    )
    
      
            as "suicide-risk"
      
    
    ,
  
    max(
      
      case
      when linkid = 'patient-age'
        then answer_value_text
      else null
      end
    )
    
      
            as "patient-age"
      
    
    ,
  
    max(
      
      case
      when linkid = 'severe-mental-health'
        then answer_value_text
      else null
      end
    )
    
      
            as "severe-mental-health"
      
    
    ,
  
    max(
      
      case
      when linkid = '59ae2c65-3a25-4acb-9db5-7aa035c54a63'
        then answer_value_text
      else null
      end
    )
    
      
            as "59ae2c65-3a25-4acb-9db5-7aa035c54a63"
      
    
    ,
  
    max(
      
      case
      when linkid = '2c50fee4-7a5d-4d62-80aa-73329934d7dc'
        then answer_value_text
      else null
      end
    )
    
      
            as "2c50fee4-7a5d-4d62-80aa-73329934d7dc"
      
    
    ,
  
    max(
      
      case
      when linkid = 'no-mental-problem'
        then answer_value_text
      else null
      end
    )
    
      
            as "no-mental-problem"
      
    
    ,
  
    max(
      
      case
      when linkid = 'alcohol-problem'
        then answer_value_text
      else null
      end
    )
    
      
            as "alcohol-problem"
      
    
    ,
  
    max(
      
      case
      when linkid = 'fe874736-efd1-4fba-b517-5560489729b0'
        then answer_value_text
      else null
      end
    )
    
      
            as "fe874736-efd1-4fba-b517-5560489729b0"
      
    
    ,
  
    max(
      
      case
      when linkid = 'patient-gender-identity'
        then answer_value_text
      else null
      end
    )
    
      
            as "patient-gender-identity"
      
    
    ,
  
    max(
      
      case
      when linkid = '156fce37-203f-4760-a9f9-1b8ab0ae0642'
        then answer_value_text
      else null
      end
    )
    
      
            as "156fce37-203f-4760-a9f9-1b8ab0ae0642"
      
    
    ,
  
    max(
      
      case
      when linkid = 'task-id-socio-pdf'
        then answer_value_text
      else null
      end
    )
    
      
            as "task-id-socio-pdf"
      
    
    ,
  
    max(
      
      case
      when linkid = '8a051272-efb0-4f1f-acc7-f5038989207f'
        then answer_value_text
      else null
      end
    )
    
      
            as "8a051272-efb0-4f1f-acc7-f5038989207f"
      
    
    ,
  
    max(
      
      case
      when linkid = '4602f806-3bf4-4f16-bee1-c2e690dca588'
        then answer_value_text
      else null
      end
    )
    
      
            as "4602f806-3bf4-4f16-bee1-c2e690dca588"
      
    
    ,
  
    max(
      
      case
      when linkid = '364e4271-e89d-4206-85a1-45ad595e4cab'
        then answer_value_text
      else null
      end
    )
    
      
            as "364e4271-e89d-4206-85a1-45ad595e4cab"
      
    
    ,
  
    max(
      
      case
      when linkid = 'patient-how-you-think-of-yourself'
        then answer_value_text
      else null
      end
    )
    
      
            as "patient-how-you-think-of-yourself"
      
    
    ,
  
    max(
      
      case
      when linkid = '97f2ed48-11d2-4417-a4b2-57a333c5157f'
        then answer_value_text
      else null
      end
    )
    
      
            as "97f2ed48-11d2-4417-a4b2-57a333c5157f"
      
    
    ,
  
    max(
      
      case
      when linkid = '91e7579d-ac74-4d72-955b-1a3675a2e656'
        then answer_value_text
      else null
      end
    )
    
      
            as "91e7579d-ac74-4d72-955b-1a3675a2e656"
      
    
    ,
  
    max(
      
      case
      when linkid = '80499f94-3f2a-4f0f-9a21-7fa252b33e29'
        then answer_value_text
      else null
      end
    )
    
      
            as "80499f94-3f2a-4f0f-9a21-7fa252b33e29"
      
    
    ,
  
    max(
      
      case
      when linkid = '69a559db-c97b-4bcb-aae4-81465fdd1d20'
        then answer_value_text
      else null
      end
    )
    
      
            as "69a559db-c97b-4bcb-aae4-81465fdd1d20"
      
    
    ,
  
    max(
      
      case
      when linkid = '99f0d10e-9522-457f-8432-fd489c53acb3'
        then answer_value_text
      else null
      end
    )
    
      
            as "99f0d10e-9522-457f-8432-fd489c53acb3"
      
    
    ,
  
    max(
      
      case
      when linkid = 'patient-pronouns'
        then answer_value_text
      else null
      end
    )
    
      
            as "patient-pronouns"
      
    
    ,
  
    max(
      
      case
      when linkid = 'b9ecb840-8515-4091-8cef-863ed297012e'
        then answer_value_text
      else null
      end
    )
    
      
            as "b9ecb840-8515-4091-8cef-863ed297012e"
      
    
    ,
  
    max(
      
      case
      when linkid = 'common-mental-health'
        then answer_value_text
      else null
      end
    )
    
      
            as "common-mental-health"
      
    
    ,
  
    max(
      
      case
      when linkid = '94f913ce-e837-48b3-906d-a0613ac72b73'
        then answer_value_text
      else null
      end
    )
    
      
            as "94f913ce-e837-48b3-906d-a0613ac72b73"
      
    
    ,
  
    max(
      
      case
      when linkid = 'a669eaea-2f12-49ff-ac91-d54009d6ab77'
        then answer_value_text
      else null
      end
    )
    
      
            as "a669eaea-2f12-49ff-ac91-d54009d6ab77"
      
    
    ,
  
    max(
      
      case
      when linkid = 'f35bbaa2-5e2e-4c74-b9b4-f0aa1eabe47d'
        then answer_value_text
      else null
      end
    )
    
      
            as "f35bbaa2-5e2e-4c74-b9b4-f0aa1eabe47d"
      
    
    ,
  
    max(
      
      case
      when linkid = 'patient-name'
        then answer_value_text
      else null
      end
    )
    
      
            as "patient-name"
      
    
    ,
  
    max(
      
      case
      when linkid = 'drug-problem'
        then answer_value_text
      else null
      end
    )
    
      
            as "drug-problem"
      
    
    ,
  
    max(
      
      case
      when linkid = '11e90302-2ae0-4e7d-be11-754b2a91ab20'
        then answer_value_text
      else null
      end
    )
    
      
            as "11e90302-2ae0-4e7d-be11-754b2a91ab20"
      
    
    ,
  
    max(
      
      case
      when linkid = 'patient-biological-sex'
        then answer_value_text
      else null
      end
    )
    
      
            as "patient-biological-sex"
      
    
    ,
  
    max(
      
      case
      when linkid = 'how-many-drinks'
        then answer_value_text
      else null
      end
    )
    
      
            as "how-many-drinks"
      
    
    ,
  
    max(
      
      case
      when linkid = 'task-id-common-mental-health-form'
        then answer_value_text
      else null
      end
    )
    
      
            as "task-id-common-mental-health-form"
      
    
    ,
  
    max(
      
      case
      when linkid = '0ee3228d-28b6-4c88-8119-808fc121aaa6'
        then answer_value_text
      else null
      end
    )
    
      
            as "0ee3228d-28b6-4c88-8119-808fc121aaa6"
      
    
    ,
  
    max(
      
      case
      when linkid = '4e9014d1-e07c-492e-897a-3bf7e8f6f018'
        then answer_value_text
      else null
      end
    )
    
      
            as "4e9014d1-e07c-492e-897a-3bf7e8f6f018"
      
    
    ,
  
    max(
      
      case
      when linkid = 'task-id-mwtool-pdf'
        then answer_value_text
      else null
      end
    )
    
      
            as "task-id-mwtool-pdf"
      
    
    ,
  
    max(
      
      case
      when linkid = 'patient-dob'
        then answer_value_text
      else null
      end
    )
    
      
            as "patient-dob"
      
    
    
  

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

  
  

