-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"

-- IPC Session 1 with readable column names
-- Questionnaire ID: 55
-- Source file: questionnaire/ipc-session-1/start-ipc-ipc-session-1.json




  
  

  
  

  
  

  
  
    
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
  

  
  

  with ids(ident) as ( values('Questionnaire/55') ),
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
      b.qr_id,max(case when linkid = '042e5415-93de-419d-9bac-632596939280' then answer_value_text end) as "person",max(case when linkid = '0627ed93-c89d-48ae-bde2-bd013dc58685' then answer_value_text end) as "place_declare_values",max(case when linkid = '07b722c9-210e-491a-8933-2e9aab3474af' then answer_value_text end) as "place_declare_values_3",max(case when linkid = '0c99c32b-a6a7-4de6-9fbc-d34ccf133bbf' then answer_value_text end) as "place_declare_values_4",max(case when linkid = '0de42ce5-6d9f-40ec-b67b-3d3b975d363b' then answer_value_text end) as "before_there_before",max(case when linkid = '10151d1b-3cd2-4f10-b1e5-d6b20112d801' then answer_value_text end) as "place_declare_values_6",max(case when linkid = '13d72f63-dd18-41f9-bf46-44981f2f804a' then answer_value_text end) as "place_declare_values_7",max(case when linkid = '16333904-aad5-4166-9593-b7286c7b20b2' then answer_value_text end) as "earlier_you_noticed",max(case when linkid = '1809cb03-235d-4855-b201-7b296b160e37' then answer_value_text end) as "there_anything_else",max(case when linkid = '19416221-7c67-44d7-9b9b-b88364ea7408' then answer_value_text end) as "ipc_session",max(case when linkid = '1ae11d83-35d4-41c0-a804-5ddf6b1c388c' then answer_value_text end) as "possible_problem_areas",max(case when linkid = '24f9ed3b-3706-43fb-9779-07a9c2077e9d' then answer_value_text end) as "place_declare_values_12",max(case when linkid = '31a9e716-52ce-4651-bb17-26f442e2ab89' then answer_value_text end) as "symptoms",max(case when linkid = '347d89f5-1bad-4ba5-8f45-e63972b02123' then answer_value_text end) as "ipc_session_14",max(case when linkid = '37a6a71f-e6ee-4b2e-bfba-2c2cd51e508b' then answer_value_text end) as "ipc_session_15",max(case when linkid = '47eaf38f-c74e-484a-af95-b0002074bf67' then answer_value_text end) as "place_declare_values_16",max(case when linkid = '4a2ac26a-8d30-4c28-be11-f684f415f661' then answer_value_text end) as "survey_response",max(case when linkid = '528fa2c7-7a88-4d4e-b2ba-38c1384aface' then answer_value_text end) as "your_most_distressing",max(case when linkid = '54a97eb0-3fd7-4b2d-b2b7-ea3fe97a1690' then answer_value_text end) as "symptoms_begin",max(case when linkid = '5d23d8f0-b23a-4e2f-ac66-497d9c6ab3fd' then answer_value_text end) as "you_notice_symptoms",max(case when linkid = '5e65c0dc-5524-4f3d-8404-a7427a34e61c' then answer_value_text end) as "symptoms_you_said",max(case when linkid = '6099951a-c965-40f9-ad61-66e7a39a6b92' then answer_value_text end) as "place_declare_values_22",max(case when linkid = '6409c231-50c1-45fc-808e-f0bd015db97d' then answer_value_text end) as "person_23",max(case when linkid = '69125945-bec7-4810-985a-8c1052e8d039' then answer_value_text end) as "patient_age",max(case when linkid = '69ed5884-0814-4ae0-a7f1-ecf730710523' then answer_value_text end) as "ipc_session_25",max(case when linkid = '78bc8272-eba1-48a8-a2f4-0e47304887b8' then answer_value_text end) as "noticed_asking_important",max(case when linkid = '798d2c43-5b9f-436f-9fa2-f360cbeeea73' then answer_value_text end) as "patient_dob",max(case when linkid = '7b0a751e-936e-40e8-9076-c7fe1e818c1f' then answer_value_text end) as "ipc_session_28",max(case when linkid = '80344998-41e2-4669-a77e-1fdaebe88bcf' then answer_value_text end) as "place_declare_values_29",max(case when linkid = '8085cded-8e0f-497f-9e3d-5fab3981d727' then answer_value_text end) as "mood_rating_ipc",max(case when linkid = '82cdd7f0-f3d1-4eba-a31e-c6ad367742f4' then answer_value_text end) as "ipc_session_31",max(case when linkid = '8a8b5e1e-fa6b-472b-ade7-ee8f5286a759' then answer_value_text end) as "place_declare_values_32",max(case when linkid = 'a11ebb0d-acb3-4038-956b-293a41acb85b-1' then answer_value_text end) as "place_declare_values_33",max(case when linkid = 'a11ebb0d-acb3-4038-956b-293a41acb85b-2' then answer_value_text end) as "place_declare_values_34",max(case when linkid = 'a88cb2bf-e23b-423b-9a22-ab94678ccc3a' then answer_value_text end) as "you_agree_summary",max(case when linkid = 'acd9d89b-0d67-415e-a3a4-b132ab304c45' then answer_value_text end) as "format_you_deliver",max(case when linkid = 'b1b6bebb-0cb7-4029-b320-01d3270ee892' then answer_value_text end) as "ipc_session_37",max(case when linkid = 'b322e52d-99f3-48e0-bda0-09d95228ae7b' then answer_value_text end) as "place_declare_values_38",max(case when linkid = 'b3e46a84-9743-4506-9465-43fa16460e16' then answer_value_text end) as "you_complete_required",max(case when linkid = 'b5bc7f80-4a0c-486c-e5eb-32c750036f94' then answer_value_text end) as "add_family_member",max(case when linkid = 'b7b59772-0272-41f6-ba66-16b09a832fc1' then answer_value_text end) as "there_anything_get",max(case when linkid = 'bd785881-7e04-4093-b191-d845edea4c9d' then answer_value_text end) as "place_declare_values_42",max(case when linkid = 'c4b93137-7abc-47f1-8c0a-fbfe8ad6ce2d' then answer_value_text end) as "going_your_life",max(case when linkid = 'calculated-month' then answer_value_text end) as "add_family_member_44",max(case when linkid = 'calculated-year' then answer_value_text end) as "add_family_member_45",max(case when linkid = 'cd69a7a1-ec17-4f43-8597-26c59732261b' then answer_value_text end) as "ipc_session_46",max(case when linkid = 'cd8e3d6d-e9ff-458d-d122-57070bebffaf' then answer_value_text end) as "add_family_member_47",max(case when linkid = 'd126d866-b8c4-4288-84ff-49a40f20e204' then answer_value_text end) as "place_declare_values_48",max(case when linkid = 'd58c8907-06c6-4071-aa6c-b2b95d2169c7' then answer_value_text end) as "mood_rating_ipc_49",max(case when linkid = 'd6c7419a-9082-4579-8678-44e866ab3655' then answer_value_text end) as "place_declare_values_50",max(case when linkid = 'e2629251-1266-4760-a539-f0e680df651b' then answer_value_text end) as "happening",max(case when linkid = 'e4ecfbd3-ed37-4b1b-9284-f7cc9386f7b8' then answer_value_text end) as "ipc_session_52",max(case when linkid = 'e5899d3e-d8fc-4931-91c8-383f51474a5b' then answer_value_text end) as "place_declare_values_53",max(case when linkid = 'e8bfb690-b135-4b0f-9291-f6247611ce52' then answer_value_text end) as "symptoms_most_distressing",max(case when linkid = 'encounter-id' then answer_value_text end) as "place_declare_values_55",max(case when linkid = 'f11ace93-5d84-4cc5-8598-58f9c5623037' then answer_value_text end) as "you_experiencing",max(case when linkid = 'f1.33.6' then answer_value_text end) as "mood_rating_ipc_57",max(case when linkid = 'f1.38.3' then answer_value_text end) as "form_question",max(case when linkid = 'f1.38.4' then answer_value_text end) as "ipc_session_59",max(case when linkid = 'f1.39.3.1' then answer_value_text end) as "person_60",max(case when linkid = 'f1.39.3.2' then answer_value_text end) as "person_61",max(case when linkid = 'f1.39.3.3' then answer_value_text end) as "person_relationship_changed",max(case when linkid = 'f1.39.3.4' then answer_value_text end) as "person_person",max(case when linkid = 'f1.39.4.1' then answer_value_text end) as "person_64",max(case when linkid = 'f1.39.4.2' then answer_value_text end) as "person_65",max(case when linkid = 'f1.39.4.3' then answer_value_text end) as "person_relationship_changed_66",max(case when linkid = 'f1.39.4.4' then answer_value_text end) as "person_person_67",max(case when linkid = 'f1.39.5.1' then answer_value_text end) as "person_68",max(case when linkid = 'f1.39.5.2' then answer_value_text end) as "person_69",max(case when linkid = 'f1.39.5.3' then answer_value_text end) as "person_relationship_changed_70",max(case when linkid = 'f1.39.5.4' then answer_value_text end) as "person_person_71",max(case when linkid = 'f1.39.6.1' then answer_value_text end) as "person_72",max(case when linkid = 'f1.39.6.2' then answer_value_text end) as "person_73",max(case when linkid = 'f1.39.6.3' then answer_value_text end) as "person_relationship_changed_74",max(case when linkid = 'f1.39.6.4' then answer_value_text end) as "person_person_75",max(case when linkid = 'f1.40.11' then answer_value_text end) as "ipc_session_76",max(case when linkid = 'f1.40.12' then answer_value_text end) as "person_consistently_close",max(case when linkid = 'f1.40.2' then answer_value_text end) as "ipc_session_78",max(case when linkid = 'f1.40.3' then answer_value_text end) as "person_consistently_close_79",max(case when linkid = 'f1.40.5' then answer_value_text end) as "ipc_session_80",max(case when linkid = 'f1.40.6' then answer_value_text end) as "person_consistently_close_81",max(case when linkid = 'f1.40.8' then answer_value_text end) as "ipc_session_82",max(case when linkid = 'f1.40.9' then answer_value_text end) as "person_consistently_close_83",max(case when linkid = 'f1.42.2' then answer_value_text end) as "you_think_important",max(case when linkid = 'f1.43.2' then answer_value_text end) as "tell_your_relationship",max(case when linkid = 'f1.46.2' then answer_value_text end) as "form_question_86",max(case when linkid = 'f1.68.2' then answer_value_text end) as "you_someone_else",max(case when linkid = 'f1.69.2' then answer_value_text end) as "briefly_tell_disagreement",max(case when linkid = 'f1.69.3' then answer_value_text end) as "disagreement_happen",max(case when linkid = 'f1.70.2' then answer_value_text end) as "disagreement_affecting_you",max(case when linkid = 'f1.72.2' then answer_value_text end) as "there_any_changes",max(case when linkid = 'f1.73.2' then answer_value_text end) as "change",max(case when linkid = 'f1.74.2' then answer_value_text end) as "happen",max(case when linkid = 'f1.76.2' then answer_value_text end) as "client_not_mention",max(case when linkid = 'f1.78.2' then answer_value_text end) as "you_say_you",max(case when linkid = 'f1.79.2' then answer_value_text end) as "anything_changed_your",max(case when linkid = 'f1.79.3' then answer_value_text end) as "event_happen",max(case when linkid = 'f1.81.3' then answer_value_text end) as "ipc_session_98",max(case when linkid = 'f1.81.4' then answer_value_text end) as "ipc_session_99",max(case when linkid = 'f1.82.2' then answer_value_text end) as "you_want_propose",max(case when linkid = 'f1.87.2' then answer_value_text end) as "you_agree",max(case when linkid = 'f1.88.2' then answer_value_text end) as "ipc_session_102",max(case when linkid = 'f1.89.2' then answer_value_text end) as "select_possible_interpersonal",max(case when linkid = 'f1.92.1' then answer_value_text end) as "ipc_session_104",max(case when linkid = 'fe689023-f019-483b-ba7c-8ff0e16129d5' then answer_value_text end) as "you_experiencing_105",max(case when linkid = 'LINK_ID_THAT_CONTAINS_ENCOUNTER_ID_AS_ANSWER' then answer_value_text end) as "LINK_ID_THAT_CONTAINS_ENCOUNTER_ID_AS_ANSWER",max(case when linkid = 'observation-mood-score-id' then answer_value_text end) as "place_declare_values_107",max(case when linkid = 'task-id-008' then answer_value_text end) as "place_declare_values_108",max(case when linkid = 'task-id-009' then answer_value_text end) as "place_declare_values_109",max(case when linkid = 'task-id-010' then answer_value_text end) as "place_declare_values_110",max(case when linkid = 'task-id-012' then answer_value_text end) as "place_declare_values_111"
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

  
  

