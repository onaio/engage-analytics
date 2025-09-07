
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_readable_start_ipc_s3__dbt_tmp"
    
    
  as (
    -- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"

-- IPC Session 3 with readable column names
-- Questionnaire ID: 208
-- Source file: questionnaire/ipc-session-3/start-ipc-ipc-session-3.json




  
  

  
  

  
  

  
  
    
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
  

  
  

  with ids(ident) as ( values('Questionnaire/208') ),
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
      b.qr_id,max(case when linkid = 'd2cdab75-7e7a-4d5a-add8-fa70bd027640-1' then answer_value_text end) as "ipc_s3_phq9_total",max(case when linkid = 'd2cdab75-7e7a-4d5a-add8-fa70bd027640-2' then answer_value_text end) as "ipc_s3_gad7_total",max(case when linkid = 'd2cdab75-7e7a-4d5a-add8-fa70bd027640-3' then answer_value_text end) as "ipc_s3_ptsd_total",max(case when linkid = 'd2cdab75-7e7a-4d5a-add8-fa70bd027640-4' then answer_value_text end) as "ipc_s3_ptsd_total_meaning",max(case when linkid = 'a11ebb0d-acb3-4038-956b-293a41acb85b-3' then answer_value_text end) as "ipc_s3_taskid_session_3_navigator",max(case when linkid = '7ff4b5cb-2c44-434b-90e9-85c61a858097' then answer_value_text end) as "ipc_s3_7ff4b5cb2c44434b90e985c61a858097",max(case when linkid = 'a11ebb0d-acb3-4038-956b-293a41acb85b-4' then answer_value_text end) as "ipc_s3_taskid_session_4_navigator",max(case when linkid = 'ac6eb49b-3d37-4bac-8d27-1e1ad01018ce' then answer_value_text end) as "ipc_s3_in_what_format_did_you_deliver_this_session_with_t",max(case when linkid = '99079ebd-1665-4503-bb37-8de493dd4a21' then answer_value_text end) as "ipc_s3_work_at_home_you_assigned_to_the_client_in_this_se",max(case when linkid = 'a24c89ff-1e14-403c-8dfa-26fc6c1103d9' then answer_value_text end) as "ipc_s3_taskid_ipc_session_3_provider_pdf",max(case when linkid = 'b3e46a84-9743-4506-9465-43fa16460e16' then answer_value_text end) as "ipc_s3_did_you_complete_the_required_assessments_and_the_",max(case when linkid = '4442589a-51b2-48c3-8297-b805e525642c' then answer_value_text end) as "ipc_s3_taskid_ipc_session_3_client_pdf",max(case when linkid = '747188c8-4fc0-43ea-92b4-9baa32567f73' then answer_value_text end) as "ipc_s3_questions_asked_in_session_2",max(case when linkid = 'encounter-id' then answer_value_text end) as "ipc_s3_encounter_id_of_ipc_session_3",max(case when linkid = 'f1.33.6' then answer_value_text end) as "ipc_s3_on_a_scale_of_1_to_10_with_1_being_the_worst_mood_",max(case when linkid = '3d2ccf9a-0e7b-48a0-8540-44d43b6dacfe' then answer_value_text end) as "ipc_s3_questions_asked_in_session_2_16",max(case when linkid = '6930d457-7bec-4983-832b-3220b163f505' then answer_value_text end) as "ipc_s3_would_you_prefer_to_focus_on_this_new_problem_area",max(case when linkid = '6fb0c7a5-a211-43ec-af7b-5f1079399133' then answer_value_text end) as "ipc_s3_questions_asked_in_session_2_18",max(case when linkid = 'observation-mood-score-id' then answer_value_text end) as "ipc_s3_observation_id_of_mood_rating_session_3_score",max(case when linkid = '1b59ddeb-483d-4b1b-af53-f3942204589a' then answer_value_text end) as "ipc_s3_1b59ddeb483d4b1baf53f3942204589a",max(case when linkid = '83176bb3-c0cb-445a-a91b-6711635b1414' then answer_value_text end) as "ipc_s3_did_you_ever_hit_kick_push_or_slap_the_personno",max(case when linkid = 'e5899d3e-d8fc-4931-91c8-383f51474a5b' then answer_value_text end) as "ipc_s3_mood_rating_total",max(case when linkid = '232484cf-1aee-4bfa-a66e-61494587a16a' then answer_value_text end) as "ipc_s3_questions_asked_in_session_2_23",max(case when linkid = '4554f4b2-d15e-47e7-a10d-d77939dd3afc' then answer_value_text end) as "ipc_s3_4554f4b2d15e47e7a10dd77939dd3afc",max(case when linkid = '9ed77f2a-0d5a-434d-b153-d8deadfbc3f8' then answer_value_text end) as "ipc_s3_questions_asked_in_session_2_25",max(case when linkid = 'f9a9f492-8eb8-4c71-8ec6-f80ab596430b' then answer_value_text end) as "ipc_s3_f9a9f4928eb84c718ec6f80ab596430b",max(case when linkid = '058278e4-33d6-4c0f-8127-ccf923bcc7ef' then answer_value_text end) as "ipc_s3_example_guiding_prompts_not_all_questions_need_to_",max(case when linkid = '1c04e4db-3d54-4898-ac8a-d9e2d1cc3e3c' then answer_value_text end) as "ipc_s3_example_guiding_prompts_not_all_questions_need_to__28",max(case when linkid = '8a126244-6ec2-4c81-a6d4-409dff788019' then answer_value_text end) as "ipc_s3_example_guiding_prompts_not_all_questions_need_to__29",max(case when linkid = '969c0120-0b86-40e5-8881-6a145fe90aa9' then answer_value_text end) as "ipc_s3_example_guiding_prompts_not_all_questions_need_to__30",max(case when linkid = '9cb58d5b-0f2c-48f0-95d2-d861ab30c2a7' then answer_value_text end) as "ipc_s3_9cb58d5b0f2c48f095d2d861ab30c2a7",max(case when linkid = '59744d45-0100-4d62-b105-5cbecd973ffd' then answer_value_text end) as "ipc_s3_59744d4501004d62b1055cbecd973ffd",max(case when linkid = '6b5fa434-2fc3-4e4b-ad11-89f6260a81f2' then answer_value_text end) as "ipc_s3_6b5fa4342fc34e4bad1189f6260a81f2",max(case when linkid = '77fc70ba-6cac-494c-b27a-77ebce2289e5' then answer_value_text end) as "ipc_s3_77fc70ba6cac494cb27a77ebce2289e5",max(case when linkid = '3f63637c-1de2-4851-a088-c3b08152447e' then answer_value_text end) as "ipc_s3_3f63637c1de24851a088c3b08152447e",max(case when linkid = '50cf6a9c-a837-4531-a13a-47381657c2b3' then answer_value_text end) as "ipc_s3_50cf6a9ca8374531a13a47381657c2b3",max(case when linkid = 'b95949ec-f2ee-4d04-9e52-fb61dac18e60' then answer_value_text end) as "ipc_s3_has_anything_changed_since_the_last_time_we_met",max(case when linkid = 'e6befba3-6d25-47ae-9cac-409dfae832a2' then answer_value_text end) as "ipc_s3_e6befba36d2547ae9cac409dfae832a2",max(case when linkid = '2fb4f2f4-23b5-4f36-b444-683563ea4fb3' then answer_value_text end) as "ipc_s3_total_score",max(case when linkid = '3bfee97a-6a0e-4cd0-bdb7-7ffa8d36191f' then answer_value_text end) as "ipc_s3_questions_asked_in_session_2_40",max(case when linkid = '4ad7384f-a332-4e41-9318-0d3f9e540ee7' then answer_value_text end) as "ipc_s3_4ad7384fa3324e4193180d3f9e540ee7",max(case when linkid = '2f7b6563-47b1-4ad4-abc3-cffdae40a57a' then answer_value_text end) as "ipc_s3_stage_identified_in_session_2",max(case when linkid = '665e1595-f169-4815-9d24-dac265f1809b' then answer_value_text end) as "ipc_s3_questions_asked_in_session_2_43",max(case when linkid = 'bc8f199a-59f0-4184-864d-b1c0befd26df' then answer_value_text end) as "ipc_s3_questions_asked_in_session_2_44",max(case when linkid = 'e17756cd-4861-4dde-859c-f90241977246' then answer_value_text end) as "ipc_s3_questions_asked_in_session_2_45",max(case when linkid = 'b24118e2-31c5-4e23-af3a-1722af72c299' then answer_value_text end) as "ipc_s3_b24118e231c54e23af3a1722af72c299",max(case when linkid = '68b3cbf5-b9ef-4da4-9ac1-bcbc20f04473' then answer_value_text end) as "ipc_s3_68b3cbf5b9ef4da49ac1bcbc20f04473",max(case when linkid = '9e995f48-b092-49b2-a9e4-c7e754e446e5' then answer_value_text end) as "ipc_s3_9e995f48b09249b2a9e4c7e754e446e5",max(case when linkid = '9f1e5102-4441-49d8-9fbc-4ea6df196b43' then answer_value_text end) as "ipc_s3_example_guiding_prompts_not_all_questions_need_to__49",max(case when linkid = 'e1755ad4-b33a-44a3-b562-4206a582c28a' then answer_value_text end) as "ipc_s3_e1755ad4b33a44a3b5624206a582c28a",max(case when linkid = 'e8b8e550-4d65-42be-acb9-2f4ac17c7f49' then answer_value_text end) as "ipc_s3_e8b8e5504d6542beacb92f4ac17c7f49",max(case when linkid = '0c9f8c1d-5827-4d21-bd33-3240fade93ac' then answer_value_text end) as "ipc_s3_example_skills",max(case when linkid = 'b31c0187-3e54-4fcf-9763-8b3f3850be92' then answer_value_text end) as "ipc_s3_example_skills_53",max(case when linkid = 'bcb8b5c0-bcd8-4a07-8fe3-9d59916e8b7c' then answer_value_text end) as "ipc_s3_bcb8b5c0bcd84a078fe39d59916e8b7c",max(case when linkid = 'eea0b1ca-5582-4b45-b5cf-36bc0665ba44' then answer_value_text end) as "ipc_s3_example_skills_55",max(case when linkid = '3e0b76f9-0902-43ba-b858-0b1505e9d168' then answer_value_text end) as "ipc_s3_questions_asked_in_session_2_56",max(case when linkid = '9cc2834b-0d28-43d0-8811-509995cfcdb8' then answer_value_text end) as "ipc_s3_questions_asked_in_session_2_57",max(case when linkid = 'a7f10195-b859-4670-abbc-a2498c4fe45f' then answer_value_text end) as "ipc_s3_questions_asked_in_session_2_58",max(case when linkid = 'f9543e3c-cbff-4d2a-b3df-bf96e509d9ca' then answer_value_text end) as "ipc_s3_f9543e3ccbff4d2ab3dfbf96e509d9ca",max(case when linkid = '6c1b2aa7-f244-4d5a-bfc6-e0faef33bfe6' then answer_value_text end) as "ipc_s3_questions_asked_in_session_2_60",max(case when linkid = '920de190-557a-4233-91a3-4bf293f18214' then answer_value_text end) as "ipc_s3_920de190557a423391a34bf293f18214",max(case when linkid = '95121ef1-bfe5-40ce-841c-524334644148' then answer_value_text end) as "ipc_s3_95121ef1bfe540ce841c524334644148",max(case when linkid = 'e1a13d53-b867-440f-8622-bccebf54f176' then answer_value_text end) as "ipc_s3_e1a13d53b867440f8622bccebf54f176",max(case when linkid = '547a07c2-f94c-4956-997f-0439626d0251' then answer_value_text end) as "ipc_s3_547a07c2f94c4956997f0439626d0251",max(case when linkid = '8ac792f5-3f3c-49d3-a800-63c662bf00cd' then answer_value_text end) as "ipc_s3_8ac792f53f3c49d3a80063c662bf00cd",max(case when linkid = '1b3394a9-eaff-4871-9537-0d29f5712adc' then answer_value_text end) as "ipc_s3_example_guiding_prompts_not_all_questions_need_to__66",max(case when linkid = '2f926bba-38b5-4b20-8852-5b9e317bb010' then answer_value_text end) as "ipc_s3_example_guiding_prompts_not_all_questions_need_to__67",max(case when linkid = '57a5ca78-4761-466a-bba4-c0c84eab5243' then answer_value_text end) as "ipc_s3_example_guiding_prompts_not_all_questions_need_to__68",max(case when linkid = 'a77f07b3-7913-4708-a354-14e503a948f1' then answer_value_text end) as "ipc_s3_a77f07b379134708a35414e503a948f1",max(case when linkid = 'e245b7d9-955a-4c4b-a107-e2bf5d6d2359' then answer_value_text end) as "ipc_s3_example_skills_70",max(case when linkid = 'eb8828db-1674-47e7-ba9a-e3c161160b65' then answer_value_text end) as "ipc_s3_questions_asked_in_session_2_71",max(case when linkid = '4c95559d-86ff-4b87-a271-e52388072a53' then answer_value_text end) as "ipc_s3_4c95559d86ff4b87a271e52388072a53",max(case when linkid = '43801cc4-c61f-4ee0-82ad-cf6f1f799593' then answer_value_text end) as "ipc_s3_example_guiding_prompts_not_all_questions_need_to__73",max(case when linkid = '451acc1a-26f4-4374-96da-09bfb47365a8' then answer_value_text end) as "ipc_s3_example_guiding_prompts_not_all_questions_need_to__74",max(case when linkid = '9082f0ba-b8e5-495a-a2a4-b30918e19d53' then answer_value_text end) as "ipc_s3_example_guiding_prompts_not_all_questions_need_to__75",max(case when linkid = 'eb78f289-5f7d-4c89-9ddf-6b393a268462' then answer_value_text end) as "ipc_s3_example_guiding_prompts_not_all_questions_need_to__76",max(case when linkid = 'bed17079-d1e5-4bbe-af07-3d7b6a86243e' then answer_value_text end) as "ipc_s3_questions_asked_in_session_2_77",max(case when linkid = '8c82f757-3f40-4b09-bdf4-78ee376f75a8' then answer_value_text end) as "ipc_s3_8c82f7573f404b09bdf478ee376f75a8",max(case when linkid = 'c91209cc-1b2c-43f0-83f0-4704418ae2d5' then answer_value_text end) as "ipc_s3_c91209cc1b2c43f083f04704418ae2d5",max(case when linkid = '55092b78-a252-44ed-9dcb-2fc78570dc8b' then answer_value_text end) as "ipc_s3_example_guiding_prompts_not_all_questions_need_to__80",max(case when linkid = '5db949bd-4ebf-4d52-b850-8e951455e975' then answer_value_text end) as "ipc_s3_example_guiding_prompts_not_all_questions_need_to__81",max(case when linkid = '601055c1-efe3-47d4-9510-1ed0cc565553' then answer_value_text end) as "ipc_s3_example_guiding_prompts_not_all_questions_need_to__82",max(case when linkid = '70e4d965-a614-4dc5-b2fa-4035f0444b19' then answer_value_text end) as "ipc_s3_example_guiding_prompts_not_all_questions_need_to__83",max(case when linkid = '0a15f38b-027a-42ff-b1e8-ca5e2b313f71' then answer_value_text end) as "0a15f38b-027a-42ff-b1e8-ca5e2b313f71",max(case when linkid = '10cd5560-6c41-41a6-94c5-af74ce96dc90' then answer_value_text end) as "10cd5560-6c41-41a6-94c5-af74ce96dc90",max(case when linkid = '4ecf12a4-a246-4b79-b281-65b3676ca3a4' then answer_value_text end) as "4ecf12a4-a246-4b79-b281-65b3676ca3a4",max(case when linkid = '86bdafab-bb90-429e-8bc5-a90dc46e7263' then answer_value_text end) as "86bdafab-bb90-429e-8bc5-a90dc46e7263",max(case when linkid = 'b5bc7f80-4a0c-486c-e5eb-32c750036f94' then answer_value_text end) as "b5bc7f80-4a0c-486c-e5eb-32c750036f94",max(case when linkid = 'calculated-month' then answer_value_text end) as "calculated-month",max(case when linkid = 'calculated-year' then answer_value_text end) as "calculated-year",max(case when linkid = 'cd8e3d6d-e9ff-458d-d122-57070bebffaf' then answer_value_text end) as "cd8e3d6d-e9ff-458d-d122-57070bebffaf",max(case when linkid = 'task-id-019' then answer_value_text end) as "task-id-019",max(case when linkid = 'task-id-020' then answer_value_text end) as "task-id-020",max(case when linkid = 'task-id-021' then answer_value_text end) as "task-id-021",max(case when linkid = 'task-id-022' then answer_value_text end) as "task-id-022",max(case when linkid = 'task-id-023' then answer_value_text end) as "task-id-023",max(case when linkid = 'task-id-024' then answer_value_text end) as "task-id-024"
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