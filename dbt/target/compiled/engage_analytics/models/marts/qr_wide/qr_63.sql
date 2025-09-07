-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"

-- IPC Session 4 (id: ipc-63, url: n/a, version: 0.0.1)




  
  

  
  

  
  

  
  
    
  

  
  

  with ids(ident) as ( values('Questionnaire/63') ),
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
      when linkid = 'a11ebb0d-acb3-4038-956b-293a41acb85b-4'
        then answer_value_text
      else null
      end
    )
    
      
            as "a11ebb0d-acb3-4038-956b-293a41acb85b-4"
      
    
    ,
  
    max(
      
      case
      when linkid = 'is-connected-to-clinical-care-details'
        then answer_value_text
      else null
      end
    )
    
      
            as "is-connected-to-clinical-care-details"
      
    
    ,
  
    max(
      
      case
      when linkid = 'is-connected-to-clinical-care-question'
        then answer_value_text
      else null
      end
    )
    
      
            as "is-connected-to-clinical-care-question"
      
    
    ,
  
    max(
      
      case
      when linkid = 'b46d08e9-6af9-47be-a09a-afd0cfdf41de'
        then answer_value_text
      else null
      end
    )
    
      
            as "b46d08e9-6af9-47be-a09a-afd0cfdf41de"
      
    
    ,
  
    max(
      
      case
      when linkid = '0b12311e-d9a3-40b4-bb18-5cc6135a3850'
        then answer_value_text
      else null
      end
    )
    
      
            as "0b12311e-d9a3-40b4-bb18-5cc6135a3850"
      
    
    ,
  
    max(
      
      case
      when linkid = 'aa8d0648-9550-489a-9543-4f00d43ba1fe'
        then answer_value_text
      else null
      end
    )
    
      
            as "aa8d0648-9550-489a-9543-4f00d43ba1fe"
      
    
    ,
  
    max(
      
      case
      when linkid = 'eda5bee4-4b2a-43d7-a6e9-a180dbf40dce-3'
        then answer_value_text
      else null
      end
    )
    
      
            as "eda5bee4-4b2a-43d7-a6e9-a180dbf40dce-3"
      
    
    ,
  
    max(
      
      case
      when linkid = 'e67ca22d-e803-4790-bbce-126f2c863490'
        then answer_value_text
      else null
      end
    )
    
      
            as "e67ca22d-e803-4790-bbce-126f2c863490"
      
    
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
      when linkid = 'eda5bee4-4b2a-43d7-a6e9-a180dbf40dce-1'
        then answer_value_text
      else null
      end
    )
    
      
            as "eda5bee4-4b2a-43d7-a6e9-a180dbf40dce-1"
      
    
    ,
  
    max(
      
      case
      when linkid = 'cb8337b0-c822-4374-895b-ee875da4d7f6'
        then answer_value_text
      else null
      end
    )
    
      
            as "cb8337b0-c822-4374-895b-ee875da4d7f6"
      
    
    ,
  
    max(
      
      case
      when linkid = '978ba6cf-b149-49e2-89c3-89a0e18461bc'
        then answer_value_text
      else null
      end
    )
    
      
            as "978ba6cf-b149-49e2-89c3-89a0e18461bc"
      
    
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
      when linkid = 'a3831bfc-0175-4e5a-8d8d-7313c7f2b69b'
        then answer_value_text
      else null
      end
    )
    
      
            as "a3831bfc-0175-4e5a-8d8d-7313c7f2b69b"
      
    
    ,
  
    max(
      
      case
      when linkid = 'a8c2cacf-7ee5-4076-af42-19d0eba0e6d6'
        then answer_value_text
      else null
      end
    )
    
      
            as "a8c2cacf-7ee5-4076-af42-19d0eba0e6d6"
      
    
    ,
  
    max(
      
      case
      when linkid = '3ce3bf84-176e-40bc-8075-1517a03c6bef'
        then answer_value_text
      else null
      end
    )
    
      
            as "3ce3bf84-176e-40bc-8075-1517a03c6bef"
      
    
    ,
  
    max(
      
      case
      when linkid = '39dfd1f4-01db-4057-9734-4f2fbd83b8fe'
        then answer_value_text
      else null
      end
    )
    
      
            as "39dfd1f4-01db-4057-9734-4f2fbd83b8fe"
      
    
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
      when linkid = '749d2a97-4195-4516-8079-60638a4d9e82'
        then answer_value_text
      else null
      end
    )
    
      
            as "749d2a97-4195-4516-8079-60638a4d9e82"
      
    
    ,
  
    max(
      
      case
      when linkid = 'ca41ccab-5f0b-4538-b394-ad342a0b5bdc'
        then answer_value_text
      else null
      end
    )
    
      
            as "ca41ccab-5f0b-4538-b394-ad342a0b5bdc"
      
    
    ,
  
    max(
      
      case
      when linkid = '4c457ea8-c60d-49f7-b166-ebabf737ab22'
        then answer_value_text
      else null
      end
    )
    
      
            as "4c457ea8-c60d-49f7-b166-ebabf737ab22"
      
    
    ,
  
    max(
      
      case
      when linkid = '7db481df-19e5-400c-a206-4811b39485d5'
        then answer_value_text
      else null
      end
    )
    
      
            as "7db481df-19e5-400c-a206-4811b39485d5"
      
    
    ,
  
    max(
      
      case
      when linkid = '936d5c53-d176-4c63-9a3a-e9e7f9db3f99'
        then answer_value_text
      else null
      end
    )
    
      
            as "936d5c53-d176-4c63-9a3a-e9e7f9db3f99"
      
    
    ,
  
    max(
      
      case
      when linkid = '19dd65c2-8709-4745-81f2-4d4843c4307f'
        then answer_value_text
      else null
      end
    )
    
      
            as "19dd65c2-8709-4745-81f2-4d4843c4307f"
      
    
    ,
  
    max(
      
      case
      when linkid = '7fef53bc-56db-47ce-8cba-8efd4bcbc318'
        then answer_value_text
      else null
      end
    )
    
      
            as "7fef53bc-56db-47ce-8cba-8efd4bcbc318"
      
    
    ,
  
    max(
      
      case
      when linkid = 'a302513b-141a-4316-9546-d013212a361e'
        then answer_value_text
      else null
      end
    )
    
      
            as "a302513b-141a-4316-9546-d013212a361e"
      
    
    ,
  
    max(
      
      case
      when linkid = '6d9345d7-61a0-425b-8496-8b08d68175d0'
        then answer_value_text
      else null
      end
    )
    
      
            as "6d9345d7-61a0-425b-8496-8b08d68175d0"
      
    
    ,
  
    max(
      
      case
      when linkid = '49a270cd-b72b-42a6-92e3-c139f7a85d74'
        then answer_value_text
      else null
      end
    )
    
      
            as "49a270cd-b72b-42a6-92e3-c139f7a85d74"
      
    
    ,
  
    max(
      
      case
      when linkid = 'dd000285-7c36-44b4-82e6-4e8826665036'
        then answer_value_text
      else null
      end
    )
    
      
            as "dd000285-7c36-44b4-82e6-4e8826665036"
      
    
    ,
  
    max(
      
      case
      when linkid = '7b65857a-5c04-4414-833d-10602f04d299'
        then answer_value_text
      else null
      end
    )
    
      
            as "7b65857a-5c04-4414-833d-10602f04d299"
      
    
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
      when linkid = 'ec39511c-de47-469d-ac44-973e3fc08532'
        then answer_value_text
      else null
      end
    )
    
      
            as "ec39511c-de47-469d-ac44-973e3fc08532"
      
    
    ,
  
    max(
      
      case
      when linkid = 'aa30ce6c-4153-46d8-a4af-83012eafa23d'
        then answer_value_text
      else null
      end
    )
    
      
            as "aa30ce6c-4153-46d8-a4af-83012eafa23d"
      
    
    ,
  
    max(
      
      case
      when linkid = 'e5899d3e-d8fc-4931-91c8-383f51474a5b'
        then answer_value_text
      else null
      end
    )
    
      
            as "e5899d3e-d8fc-4931-91c8-383f51474a5b"
      
    
    ,
  
    max(
      
      case
      when linkid = '9b602b02-e5f9-4795-9fb9-fc7ae4353a19'
        then answer_value_text
      else null
      end
    )
    
      
            as "9b602b02-e5f9-4795-9fb9-fc7ae4353a19"
      
    
    ,
  
    max(
      
      case
      when linkid = '83a63af9-6f49-4fad-bca9-80776fc97be7'
        then answer_value_text
      else null
      end
    )
    
      
            as "83a63af9-6f49-4fad-bca9-80776fc97be7"
      
    
    ,
  
    max(
      
      case
      when linkid = '9a02423f-9848-4254-b427-3dfa8819d0de'
        then answer_value_text
      else null
      end
    )
    
      
            as "9a02423f-9848-4254-b427-3dfa8819d0de"
      
    
    ,
  
    max(
      
      case
      when linkid = 'b3e46a84-9743-4506-9465-43fa16460e16'
        then answer_value_text
      else null
      end
    )
    
      
            as "b3e46a84-9743-4506-9465-43fa16460e16"
      
    
    ,
  
    max(
      
      case
      when linkid = '05617430-b835-4ebd-8d6a-ff58ef6d2824'
        then answer_value_text
      else null
      end
    )
    
      
            as "05617430-b835-4ebd-8d6a-ff58ef6d2824"
      
    
    ,
  
    max(
      
      case
      when linkid = '9848888d-1ec0-4d43-b53f-7613ba5144fe'
        then answer_value_text
      else null
      end
    )
    
      
            as "9848888d-1ec0-4d43-b53f-7613ba5144fe"
      
    
    ,
  
    max(
      
      case
      when linkid = 'eda5bee4-4b2a-43d7-a6e9-a180dbf40dce-2'
        then answer_value_text
      else null
      end
    )
    
      
            as "eda5bee4-4b2a-43d7-a6e9-a180dbf40dce-2"
      
    
    ,
  
    max(
      
      case
      when linkid = '4e9014d1-e07c-492e-897a-3bf7e8f6f018'
        then answer_value_text
      else null
      end
    )
    
      
            as "4e9014d1-e07c-492e-897a-3bf7e8f6f018"
      
    
    
  

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

  
  

