-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"

-- IPC Session 2 with readable column names
-- Questionnaire ID: 58




  
  

  
  

  
  

  
  
    
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
  

  
  

  with ids(ident) as ( values('Questionnaire/58') ),
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
      b.qr_id,max(case when linkid = '0a15f38b-027a-42ff-b1e8-ca5e2b313f71' then answer_value_text end) as "ipc_s2_phq9_total_meaning",max(case when linkid = '10cd5560-6c41-41a6-94c5-af74ce96dc90' then answer_value_text end) as "ipc_s2_gad7_total_meaning",max(case when linkid = '6099951a-c965-40f9-ad61-66e7a39a6b92' then answer_value_text end) as "ipc_s2_ptsd_total",max(case when linkid = '8feb859b-ec9b-40c9-b576-9519401808ef' then answer_value_text end) as "ipc_s2_phq9_total",max(case when linkid = '8feb859b-ec9b-40c9-b576-9519401808ef-2' then answer_value_text end) as "ipc_s2_summary_when_1",max(case when linkid = '8feb859b-ec9b-40c9-b576-9519401808ef-3' then answer_value_text end) as "ipc_s2_most_distressing_symptoms",max(case when linkid = '8feb859b-ec9b-40c9-b576-9519401808ef-4' then answer_value_text end) as "ipc_s2_selected_problem_area",max(case when linkid = '94e90f68-ea59-4837-b0ad-452f5f0336c3' then answer_value_text end) as "ipc_s2_ptsd_total_meaning_toggle",max(case when linkid = '96171fdd-a424-4c09-91e8-31986ac315f7' then answer_value_text end) as "ipc_s2_gad7_total",max(case when linkid = 'a11ebb0d-acb3-4038-956b-293a41acb85b-2' then answer_value_text end) as "ipc_s2_taskid_session_2_navigator",max(case when linkid = '5d0b76c5-a416-45a4-a8fc-9889ea840a03' then answer_value_text end) as "ipc_s2_in_what_format_did_you_deliver_this_session_with_t",max(case when linkid = '7ff4b5cb-2c44-434b-90e9-85c61a858097' then answer_value_text end) as "ipc_s2_7ff4b5cb2c44434b90e985c61a858097",max(case when linkid = 'a11ebb0d-acb3-4038-956b-293a41acb85b-3' then answer_value_text end) as "ipc_s2_taskid_session_3_navigator",max(case when linkid = '301ae5ab-7dac-4414-a80a-f8784ba6bf51' then answer_value_text end) as "ipc_s2_have_you_proposed_the_interpersonal_formulation_in",max(case when linkid = '3abecf2a-c87b-4fad-96eb-e9abc37e9c54' then answer_value_text end) as "ipc_s2_taskid_ipc_session_2_provider_pdf",max(case when linkid = '99079ebd-1665-4503-bb37-8de493dd4a21' then answer_value_text end) as "ipc_s2_work_at_home_you_assigned_to_the_client_in_this_se",max(case when linkid = 'a4a9cad5-04db-44e5-9a9d-0f1bf40ca840' then answer_value_text end) as "ipc_s2_a4a9cad504db44e59a9d0f1bf40ca840",max(case when linkid = 'b3e46a84-9743-4506-9465-43fa16460e16' then answer_value_text end) as "ipc_s2_did_you_complete_the_required_assessments_and_the_",max(case when linkid = '6c0d6638-9b65-4016-b3f0-4008f8e462c2' then answer_value_text end) as "ipc_s2_taskid_ipc_session_2_client_pdf",max(case when linkid = '747188c8-4fc0-43ea-92b4-9baa32567f73' then answer_value_text end) as "ipc_s2_747188c84fc043ea92b49baa32567f73",max(case when linkid = 'ac0ade27-9e9a-42a9-8ec6-4e7e79830299' then answer_value_text end) as "ipc_s2_would_you_prefer_to_focus_on_this_new_problem_area",max(case when linkid = 'd8eb3c09-b97d-4d55-9590-e6ce932a7a10' then answer_value_text end) as "ipc_s2_did_the_person_ever_hit_kick_push_or_slap_you",max(case when linkid = '62cd085f-52d4-44de-881c-93058f201559' then answer_value_text end) as "ipc_s2_do_you_agree_with_this",max(case when linkid = 'encounter-id' then answer_value_text end) as "ipc_s2_encounter_id_of_ipc_session_2",max(case when linkid = 'f1.33.6' then answer_value_text end) as "ipc_s2_on_a_scale_of_1_to_10_with_1_being_the_worst_mood_",max(case when linkid = '2efaba4d-f5af-45f3-9340-1ba9b3fffc1c' then answer_value_text end) as "ipc_s2_was_this_in_the_context_of_an_argument_or_disagree",max(case when linkid = '3d2ccf9a-0e7b-48a0-8540-44d43b6dacfe' then answer_value_text end) as "ipc_s2_3d2ccf9a0e7b48a0854044d43b6dacfe",max(case when linkid = '6930d457-7bec-4983-832b-3220b163f505' then answer_value_text end) as "ipc_s2_would_you_prefer_to_focus_on_this_new_problem_area_28",max(case when linkid = '6fb0c7a5-a211-43ec-af7b-5f1079399133' then answer_value_text end) as "ipc_s2_6fb0c7a5a21143ecaf7b5f1079399133",max(case when linkid = 'observation-mood-score-id' then answer_value_text end) as "ipc_s2_observation_id_of_mood_rating_session_1_score",max(case when linkid = '83176bb3-c0cb-445a-a91b-6711635b1414' then answer_value_text end) as "ipc_s2_did_you_ever_hit_kick_push_or_slap_the_person",max(case when linkid = 'a9314459-f6d9-4ebb-84e7-f1a1968cfd7c' then answer_value_text end) as "ipc_s2_do_you_still_think_that",max(case when linkid = 'e5899d3e-d8fc-4931-91c8-383f51474a5b' then answer_value_text end) as "ipc_s2_mood_rating_total",max(case when linkid = '232484cf-1aee-4bfa-a66e-61494587a16a' then answer_value_text end) as "ipc_s2_232484cf1aee4bfaa66e61494587a16a",max(case when linkid = '9ed77f2a-0d5a-434d-b153-d8deadfbc3f8' then answer_value_text end) as "ipc_s2_9ed77f2a0d5a434db153d8deadfbc3f8",max(case when linkid = '3bfee97a-6a0e-4cd0-bdb7-7ffa8d36191f' then answer_value_text end) as "ipc_s2_3bfee97a6a0e4cd0bdb77ffa8d36191f",max(case when linkid = '62c9e3f2-30aa-46b0-9190-aa36a1804fdc' then answer_value_text end) as "ipc_s2_was_this_in_the_context_of_an_argument_or_disagree_37",max(case when linkid = '9eac8fed-4265-447f-bbde-0986c34f3875' then answer_value_text end) as "ipc_s2_example_guiding_prompts_not_all_questions_need_to_",max(case when linkid = 'b93eafea-8b01-4b21-994b-c45113622a93' then answer_value_text end) as "ipc_s2_example_guiding_prompts_not_all_questions_need_to__39",max(case when linkid = 'ecd82c04-48de-48ec-8efe-24cbe04cbe4b' then answer_value_text end) as "ipc_s2_example_guiding_prompts_not_all_questions_need_to__40",max(case when linkid = 'f82f76f4-648a-453a-916d-28795448a758' then answer_value_text end) as "ipc_s2_example_guiding_prompts_not_all_questions_need_to__41",max(case when linkid = '287593c3-4174-45af-a998-71d249230943' then answer_value_text end) as "ipc_s2_287593c3417445afa99871d249230943",max(case when linkid = '9cb58d5b-0f2c-48f0-95d2-d861ab30c2a7' then answer_value_text end) as "ipc_s2_9cb58d5b0f2c48f095d2d861ab30c2a7",max(case when linkid = '3f63637c-1de2-4851-a088-c3b08152447e' then answer_value_text end) as "ipc_s2_3f63637c1de24851a088c3b08152447e",max(case when linkid = '50cf6a9c-a837-4531-a13a-47381657c2b3' then answer_value_text end) as "ipc_s2_50cf6a9ca8374531a13a47381657c2b3",max(case when linkid = '665e1595-f169-4815-9d24-dac265f1809b' then answer_value_text end) as "ipc_s2_665e1595f16948159d24dac265f1809b",max(case when linkid = 'bc8f199a-59f0-4184-864d-b1c0befd26df' then answer_value_text end) as "ipc_s2_bc8f199a59f04184864db1c0befd26df",max(case when linkid = 'e1755ad4-b33a-44a3-b562-4206a582c28a' then answer_value_text end) as "ipc_s2_e1755ad4b33a44a3b5624206a582c28a",max(case when linkid = 'e17756cd-4861-4dde-859c-f90241977246' then answer_value_text end) as "ipc_s2_e17756cd48614dde859cf90241977246",max(case when linkid = 'e6befba3-6d25-47ae-9cac-409dfae832a2' then answer_value_text end) as "ipc_s2_e6befba36d2547ae9cac409dfae832a2",max(case when linkid = '6ff6f541-cb58-48d3-b7eb-24ab667f0a94' then answer_value_text end) as "ipc_s2_is_any_of_the_2_question_above_is_true",max(case when linkid = '85a3f14c-e4d8-4c45-9088-967ec4ea0d07' then answer_value_text end) as "ipc_s2_total_score",max(case when linkid = '2ef63cf4-b37e-4216-a8e1-db6d1b91cc2a' then answer_value_text end) as "ipc_s2_2ef63cf4b37e4216a8e1db6d1b91cc2a",max(case when linkid = '3e0b76f9-0902-43ba-b858-0b1505e9d168' then answer_value_text end) as "ipc_s2_3e0b76f9090243bab8580b1505e9d168",max(case when linkid = '9cc2834b-0d28-43d0-8811-509995cfcdb8' then answer_value_text end) as "ipc_s2_9cc2834b0d2843d08811509995cfcdb8",max(case when linkid = 'a7f10195-b859-4670-abbc-a2498c4fe45f' then answer_value_text end) as "ipc_s2_a7f10195b8594670abbca2498c4fe45f",max(case when linkid = '0b80fd6e-3e52-4d1f-aa19-bbfdd912d724' then answer_value_text end) as "ipc_s2_example_guiding_prompts_not_all_questions_need_to__57",max(case when linkid = 'f9543e3c-cbff-4d2a-b3df-bf96e509d9ca' then answer_value_text end) as "ipc_s2_f9543e3ccbff4d2ab3dfbf96e509d9ca",max(case when linkid = '547a07c2-f94c-4956-997f-0439626d0251' then answer_value_text end) as "ipc_s2_547a07c2f94c4956997f0439626d0251",max(case when linkid = '6c1b2aa7-f244-4d5a-bfc6-e0faef33bfe6' then answer_value_text end) as "ipc_s2_6c1b2aa7f2444d5abfc6e0faef33bfe6",max(case when linkid = 'a77f07b3-7913-4708-a354-14e503a948f1' then answer_value_text end) as "ipc_s2_a77f07b379134708a35414e503a948f1",max(case when linkid = '0c9f8c1d-5827-4d21-bd33-3240fade93ac' then answer_value_text end) as "ipc_s2_example_skills",max(case when linkid = 'b31c0187-3e54-4fcf-9763-8b3f3850be92' then answer_value_text end) as "ipc_s2_example_skills_63",max(case when linkid = 'bcb8b5c0-bcd8-4a07-8fe3-9d59916e8b7c' then answer_value_text end) as "ipc_s2_bcb8b5c0bcd84a078fe39d59916e8b7c",max(case when linkid = 'eb8828db-1674-47e7-ba9a-e3c161160b65' then answer_value_text end) as "ipc_s2_eb8828db167447e7ba9ae3c161160b65",max(case when linkid = 'eea0b1ca-5582-4b45-b5cf-36bc0665ba44' then answer_value_text end) as "ipc_s2_example_skills_66",max(case when linkid = '4c95559d-86ff-4b87-a271-e52388072a53' then answer_value_text end) as "ipc_s2_4c95559d86ff4b87a271e52388072a53",max(case when linkid = 'bed17079-d1e5-4bbe-af07-3d7b6a86243e' then answer_value_text end) as "ipc_s2_bed17079d1e54bbeaf073d7b6a86243e",max(case when linkid = '0984569b-3fd6-4c4e-a391-e68e73b46f00' then answer_value_text end) as "ipc_s2_example_guiding_prompts_not_all_questions_need_to__69",max(case when linkid = '2a5d7010-a84b-414c-adcb-ad0f49a95740' then answer_value_text end) as "ipc_s2_example_guiding_prompts_not_all_questions_need_to__70",max(case when linkid = '63ac8d07-518c-4791-849a-a9e2c282ce9a' then answer_value_text end) as "ipc_s2_example_guiding_prompts_not_all_questions_need_to__71",max(case when linkid = 'e245b7d9-955a-4c4b-a107-e2bf5d6d2359' then answer_value_text end) as "ipc_s2_example_skills_72",max(case when linkid = 'c91209cc-1b2c-43f0-83f0-4704418ae2d5' then answer_value_text end) as "ipc_s2_c91209cc1b2c43f083f04704418ae2d5",max(case when linkid = '0e5c44d8-3ec0-427a-99a5-58c2c9cdf9c3' then answer_value_text end) as "ipc_s2_example_guiding_prompts_not_all_questions_need_to__74",max(case when linkid = '126c7309-8143-4c33-857f-41cc6bed09d8' then answer_value_text end) as "ipc_s2_example_guiding_prompts_not_all_questions_need_to__75",max(case when linkid = '2f9608db-8796-4f45-89b7-741585b8a362' then answer_value_text end) as "ipc_s2_example_guiding_prompts_not_all_questions_need_to__76",max(case when linkid = '5817d327-373e-4fc0-9bac-b64e408b0e7f' then answer_value_text end) as "ipc_s2_example_guiding_prompts_not_all_questions_need_to__77",max(case when linkid = '044d9000-1935-4b35-9947-d668de09f10b' then answer_value_text end) as "ipc_s2_example_guiding_prompts_not_all_questions_need_to__78",max(case when linkid = '52f7924d-1c66-4049-8611-26d4e7bcb260' then answer_value_text end) as "ipc_s2_example_guiding_prompts_not_all_questions_need_to__79",max(case when linkid = 'e73f462c-e543-4a3d-849d-dbedcb831444' then answer_value_text end) as "ipc_s2_example_guiding_prompts_not_all_questions_need_to__80",max(case when linkid = 'f39b0b39-2058-4c29-a183-aac5dc70e083' then answer_value_text end) as "ipc_s2_example_guiding_prompts_not_all_questions_need_to__81",max(case when linkid = '86bdafab-bb90-429e-8bc5-a90dc46e7263' then answer_value_text end) as "86bdafab-bb90-429e-8bc5-a90dc46e7263",max(case when linkid = 'b5bc7f80-4a0c-486c-e5eb-32c750036f94' then answer_value_text end) as "b5bc7f80-4a0c-486c-e5eb-32c750036f94",max(case when linkid = 'calculated-month' then answer_value_text end) as "calculated-month",max(case when linkid = 'calculated-year' then answer_value_text end) as "calculated-year",max(case when linkid = 'cd8e3d6d-e9ff-458d-d122-57070bebffaf' then answer_value_text end) as "cd8e3d6d-e9ff-458d-d122-57070bebffaf",max(case when linkid = 'task-id-013' then answer_value_text end) as "task-id-013",max(case when linkid = 'task-id-014' then answer_value_text end) as "task-id-014",max(case when linkid = 'task-id-015' then answer_value_text end) as "task-id-015",max(case when linkid = 'task-id-016' then answer_value_text end) as "task-id-016",max(case when linkid = 'task-id-018' then answer_value_text end) as "task-id-018"
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

  
  

