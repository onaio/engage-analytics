
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_planning_next_steps__dbt_tmp"
    
    
  as (
    -- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"

-- Planning Next Steps with readable column names
-- Questionnaire ID: q-planning-next-steps
-- Source file: questionnaire/assessments/planning-next-steps.json




  
  

  
  

  
  

  
  
    
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
  

  
  

  with ids(ident) as ( values('Questionnaire/q-planning-next-steps') ),
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
      b.qr_id,max(case when linkid = '12894a06-d244-4426-06f5-f2a3b4c5d6e7' then answer_value_text end) as "survey_response",max(case when linkid = '1d2fdd10-a4d1-43e0-8042-8772146e63f7' then answer_value_text end) as "survey_response_2",max(case when linkid = '256b56d8-c4c2-4313-9e2d-6720941be85f' then answer_value_text end) as "survey_response_3",max(case when linkid = '2cb66889-3c74-40ff-be3d-183fdb17ce2d' then answer_value_text end) as "survey_response_4",max(case when linkid = '34a56c28-f466-4648-2817-41b2c3d4e5f6' then answer_value_text end) as "survey_response_5",max(case when linkid = '34ab6c28-f466-4648-2817-14c5d6e7f8a9' then answer_value_text end) as "survey_response_6",max(case when linkid = '34ab6c28-f466-4648-2817-1eacbdcedf01' then answer_value_text end) as "survey_response_7",max(case when linkid = '3b9f7e5d-9a66-4c4d-8e1d-4d5e6f7a8b9d' then answer_value_text end) as "survey_response_8",max(case when linkid = '3d373ba7-14fa-43bb-bd27-52fad3d2783f' then answer_value_text end) as "survey_response_9",max(case when linkid = '45b67d39-0577-4759-3928-52c3d4e5f6a7' then answer_value_text end) as "survey_response_10",max(case when linkid = '45bc7d39-0577-4759-3928-2fbdcedf0123' then answer_value_text end) as "survey_response_11",max(case when linkid = '4ca08f6e-0b77-4d5e-9f2e-5e6f7a8b9cae' then answer_value_text end) as "survey_response_12",max(case when linkid = '56cd8e4a-1688-486a-4a39-3acedf012345' then answer_value_text end) as "survey_response_13",max(case when linkid = '5db19a7f-1c88-4e6f-8a3f-6f7a8b9c0dbf' then answer_value_text end) as "survey_response_14",max(case when linkid = '69712544-6237-4962-a455-ee6e9ee80000' then answer_value_text end) as "survey_response_15",max(case when linkid = '76ba8dbe-5456-4de4-bdde-4b8059ecfa0e' then answer_value_text end) as "survey_response_16",max(case when linkid = '9311146c-720d-47f6-b902-aeecaf3c0574' then answer_value_text end) as "survey_response_17",max(case when linkid = '950d904a-da1a-4ccd-988d-ea6b463ee106' then answer_value_text end) as "survey_response_18",max(case when linkid = '9a8b7c6d-5e4f-4a3b-2c1d-0e9f8a7b6c5d' then answer_value_text end) as "survey_response_19",max(case when linkid = 'a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d' then answer_value_text end) as "survey_response_20",max(case when linkid = 'ab12d39f-6bdd-4dbf-9f8e-8f13456789ab' then answer_value_text end) as "survey_response_21",max(case when linkid = 'b1c2d3e4-f5a6-4b7c-8d9e-0f1a2b3c4d5e' then answer_value_text end) as "survey_response_22",max(case when linkid = 'b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e' then answer_value_text end) as "survey_response_23",max(case when linkid = 'bc23e4a0-7cee-4ec0-a09f-902456789abc' then answer_value_text end) as "survey_response_24",max(case when linkid = 'bc2def4a-7cee-4ec0-a09f-cf3a4b5c6d7e' then answer_value_text end) as "survey_response_25",max(case when linkid = 'c3d4e5f6-a7b8-4c9d-0e1f-2a3b4c5d6e7f' then answer_value_text end) as "survey_response_26",max(case when linkid = 'c9d8b7a6-f5e4-4d3c-b2a1-09f8e7d6c5b4' then answer_value_text end) as "survey_response_27",max(case when linkid = 'cd34f5b1-8dff-4fd1-b1a0-a1356789abcd' then answer_value_text end) as "survey_response_28",max(case when linkid = 'cd34f5b1-8dff-4fd1-b1a0-ad3b4c5d6e7f' then answer_value_text end) as "survey_response_29",max(case when linkid = 'cd3ef5b1-8dff-4fd1-b1a0-da4b5c6d7e8f' then answer_value_text end) as "survey_response_30",max(case when linkid = 'd0e1f2a3-b4c5-4d6e-7f8a-9b0c1d2e3f4a' then answer_value_text end) as "survey_response_31",max(case when linkid = 'd4e5f6a7-b8c9-4d0e-1f2a-3b4c5d6e7f8g' then answer_value_text end) as "q_d4e5f6a7",max(case when linkid = 'de4506c2-9e00-40e2-c2b1-be4c5d6e7f8a' then answer_value_text end) as "survey_response_33",max(case when linkid = 'de4f06c2-9e00-40e2-c2b1-eb5c6d7e8f90' then answer_value_text end) as "survey_response_34",max(case when linkid = 'e5f6a7b8-c9d0-4e1f-2a3b-4c5d6e7f8g9h' then answer_value_text end) as "q_e5f6a7b8",max(case when linkid = 'e6a2b8e5-3d5f-4f5a-9a3d-f2e1c0b9a8d7' then answer_value_text end) as "survey_response_36",max(case when linkid = 'ef5617d3-af11-41f3-d3c2-cf5d6e7f8a9b' then answer_value_text end) as "survey_response_37",max(case when linkid = 'ef5617d3-af11-41f3-d3c2-fcd0e1f2a3b4' then answer_value_text end) as "survey_response_38",max(case when linkid = 'f06728e4-b022-4204-e4d3-d0e1f2a3b4c5' then answer_value_text end) as "survey_response_39",max(case when linkid = 'f4e3d2c1-b0a9-4988-7766-554433221100' then answer_value_text end) as "survey_response_40",max(case when linkid = 'f6a7b8c9-d0e1-4f2a-3b4c-5d6e7f8g9h0i' then answer_value_text end) as "q_f6a7b8c9",max(case when linkid = 'fe874736-efd1-4fba-b517-5560489729b0' then answer_value_text end) as "supervisor_recommendation_form",max(case when linkid = 'financial-hardships-list' then answer_value_text end) as "financial_hardships_list",max(case when linkid = 'is-fws' then answer_value_text end) as "fws",max(case when linkid = 'is-ipc' then answer_value_text end) as "ipc",max(case when linkid = 'is-ipc-main' then answer_value_text end) as "ipc_main",max(case when linkid = 'is-ipc-optional' then answer_value_text end) as "ipc_optional",max(case when linkid = 'is-mhs' then answer_value_text end) as "mhs",max(case when linkid = 'is-other-agency-services' then answer_value_text end) as "other_agency_services",max(case when linkid = 'is-other-services-outside-agency' then answer_value_text end) as "other_services_outside",max(case when linkid = 'is-sbirt' then answer_value_text end) as "sbirt",max(case when linkid = 'is-sbirt-main' then answer_value_text end) as "sbirt_main",max(case when linkid = 'is-sbirt-optional' then answer_value_text end) as "sbirt_optional",max(case when linkid = 'is-spi' then answer_value_text end) as "spi",max(case when linkid = 'patient-age' then answer_value_text end) as "patient_age",max(case when linkid = 'patient-dob' then answer_value_text end) as "patient_dob",max(case when linkid = 'patient-name' then answer_value_text end) as "patient_name",max(case when linkid = 'scheduled-ipc-session-1-with-current-time' then answer_value_text end) as "scheduled_ipc_session",max(case when linkid = 'scheduled-ipc-session-1-with-current-time-2' then answer_value_text end) as "scheduled_ipc_session_59",max(case when linkid = 'task-id-000' then answer_value_text end) as "task",max(case when linkid = 'task-id-001' then answer_value_text end) as "task_61",max(case when linkid = 'task-id-002' then answer_value_text end) as "task_62",max(case when linkid = 'task-id-003' then answer_value_text end) as "task_63",max(case when linkid = 'task-id-004' then answer_value_text end) as "task_64",max(case when linkid = 'task-id-006' then answer_value_text end) as "task_65",max(case when linkid = 'task-id-planning-next-steps-pdf' then answer_value_text end) as "task_planning_next"
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