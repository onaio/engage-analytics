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
      b.qr_id,max(case when linkid = '12894a06-d244-4426-06f5-f2a3b4c5d6e7' then answer_value_text end) as "12894a06-d244-4426-06f5-f2a3b4c5d6e7",max(case when linkid = '1d2fdd10-a4d1-43e0-8042-8772146e63f7' then answer_value_text end) as "1d2fdd10-a4d1-43e0-8042-8772146e63f7",max(case when linkid = '256b56d8-c4c2-4313-9e2d-6720941be85f' then answer_value_text end) as "256b56d8-c4c2-4313-9e2d-6720941be85f",max(case when linkid = '2cb66889-3c74-40ff-be3d-183fdb17ce2d' then answer_value_text end) as "2cb66889-3c74-40ff-be3d-183fdb17ce2d",max(case when linkid = '34a56c28-f466-4648-2817-41b2c3d4e5f6' then answer_value_text end) as "34a56c28-f466-4648-2817-41b2c3d4e5f6",max(case when linkid = '34ab6c28-f466-4648-2817-14c5d6e7f8a9' then answer_value_text end) as "34ab6c28-f466-4648-2817-14c5d6e7f8a9",max(case when linkid = '34ab6c28-f466-4648-2817-1eacbdcedf01' then answer_value_text end) as "34ab6c28-f466-4648-2817-1eacbdcedf01",max(case when linkid = '3b9f7e5d-9a66-4c4d-8e1d-4d5e6f7a8b9d' then answer_value_text end) as "3b9f7e5d-9a66-4c4d-8e1d-4d5e6f7a8b9d",max(case when linkid = '3d373ba7-14fa-43bb-bd27-52fad3d2783f' then answer_value_text end) as "3d373ba7-14fa-43bb-bd27-52fad3d2783f",max(case when linkid = '45b67d39-0577-4759-3928-52c3d4e5f6a7' then answer_value_text end) as "45b67d39-0577-4759-3928-52c3d4e5f6a7",max(case when linkid = '45bc7d39-0577-4759-3928-2fbdcedf0123' then answer_value_text end) as "45bc7d39-0577-4759-3928-2fbdcedf0123",max(case when linkid = '4ca08f6e-0b77-4d5e-9f2e-5e6f7a8b9cae' then answer_value_text end) as "4ca08f6e-0b77-4d5e-9f2e-5e6f7a8b9cae",max(case when linkid = '56cd8e4a-1688-486a-4a39-3acedf012345' then answer_value_text end) as "56cd8e4a-1688-486a-4a39-3acedf012345",max(case when linkid = '5db19a7f-1c88-4e6f-8a3f-6f7a8b9c0dbf' then answer_value_text end) as "5db19a7f-1c88-4e6f-8a3f-6f7a8b9c0dbf",max(case when linkid = '69712544-6237-4962-a455-ee6e9ee80000' then answer_value_text end) as "69712544-6237-4962-a455-ee6e9ee80000",max(case when linkid = '76ba8dbe-5456-4de4-bdde-4b8059ecfa0e' then answer_value_text end) as "76ba8dbe-5456-4de4-bdde-4b8059ecfa0e",max(case when linkid = '9311146c-720d-47f6-b902-aeecaf3c0574' then answer_value_text end) as "9311146c-720d-47f6-b902-aeecaf3c0574",max(case when linkid = '950d904a-da1a-4ccd-988d-ea6b463ee106' then answer_value_text end) as "950d904a-da1a-4ccd-988d-ea6b463ee106",max(case when linkid = '9a8b7c6d-5e4f-4a3b-2c1d-0e9f8a7b6c5d' then answer_value_text end) as "9a8b7c6d-5e4f-4a3b-2c1d-0e9f8a7b6c5d",max(case when linkid = 'a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d' then answer_value_text end) as "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d",max(case when linkid = 'ab12d39f-6bdd-4dbf-9f8e-8f13456789ab' then answer_value_text end) as "ab12d39f-6bdd-4dbf-9f8e-8f13456789ab",max(case when linkid = 'b1c2d3e4-f5a6-4b7c-8d9e-0f1a2b3c4d5e' then answer_value_text end) as "b1c2d3e4-f5a6-4b7c-8d9e-0f1a2b3c4d5e",max(case when linkid = 'b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e' then answer_value_text end) as "b2c3d4e5-f6a7-4b8c-9d0e-1f2a3b4c5d6e",max(case when linkid = 'bc23e4a0-7cee-4ec0-a09f-902456789abc' then answer_value_text end) as "bc23e4a0-7cee-4ec0-a09f-902456789abc",max(case when linkid = 'bc2def4a-7cee-4ec0-a09f-cf3a4b5c6d7e' then answer_value_text end) as "bc2def4a-7cee-4ec0-a09f-cf3a4b5c6d7e",max(case when linkid = 'c3d4e5f6-a7b8-4c9d-0e1f-2a3b4c5d6e7f' then answer_value_text end) as "c3d4e5f6-a7b8-4c9d-0e1f-2a3b4c5d6e7f",max(case when linkid = 'c9d8b7a6-f5e4-4d3c-b2a1-09f8e7d6c5b4' then answer_value_text end) as "c9d8b7a6-f5e4-4d3c-b2a1-09f8e7d6c5b4",max(case when linkid = 'cd34f5b1-8dff-4fd1-b1a0-a1356789abcd' then answer_value_text end) as "cd34f5b1-8dff-4fd1-b1a0-a1356789abcd",max(case when linkid = 'cd34f5b1-8dff-4fd1-b1a0-ad3b4c5d6e7f' then answer_value_text end) as "cd34f5b1-8dff-4fd1-b1a0-ad3b4c5d6e7f",max(case when linkid = 'cd3ef5b1-8dff-4fd1-b1a0-da4b5c6d7e8f' then answer_value_text end) as "cd3ef5b1-8dff-4fd1-b1a0-da4b5c6d7e8f",max(case when linkid = 'd0e1f2a3-b4c5-4d6e-7f8a-9b0c1d2e3f4a' then answer_value_text end) as "d0e1f2a3-b4c5-4d6e-7f8a-9b0c1d2e3f4a",max(case when linkid = 'd4e5f6a7-b8c9-4d0e-1f2a-3b4c5d6e7f8g' then answer_value_text end) as "d4e5f6a7-b8c9-4d0e-1f2a-3b4c5d6e7f8g",max(case when linkid = 'de4506c2-9e00-40e2-c2b1-be4c5d6e7f8a' then answer_value_text end) as "de4506c2-9e00-40e2-c2b1-be4c5d6e7f8a",max(case when linkid = 'de4f06c2-9e00-40e2-c2b1-eb5c6d7e8f90' then answer_value_text end) as "de4f06c2-9e00-40e2-c2b1-eb5c6d7e8f90",max(case when linkid = 'e5f6a7b8-c9d0-4e1f-2a3b-4c5d6e7f8g9h' then answer_value_text end) as "e5f6a7b8-c9d0-4e1f-2a3b-4c5d6e7f8g9h",max(case when linkid = 'e6a2b8e5-3d5f-4f5a-9a3d-f2e1c0b9a8d7' then answer_value_text end) as "e6a2b8e5-3d5f-4f5a-9a3d-f2e1c0b9a8d7",max(case when linkid = 'ef5617d3-af11-41f3-d3c2-cf5d6e7f8a9b' then answer_value_text end) as "ef5617d3-af11-41f3-d3c2-cf5d6e7f8a9b",max(case when linkid = 'ef5617d3-af11-41f3-d3c2-fcd0e1f2a3b4' then answer_value_text end) as "ef5617d3-af11-41f3-d3c2-fcd0e1f2a3b4",max(case when linkid = 'f06728e4-b022-4204-e4d3-d0e1f2a3b4c5' then answer_value_text end) as "f06728e4-b022-4204-e4d3-d0e1f2a3b4c5",max(case when linkid = 'f4e3d2c1-b0a9-4988-7766-554433221100' then answer_value_text end) as "f4e3d2c1-b0a9-4988-7766-554433221100",max(case when linkid = 'f6a7b8c9-d0e1-4f2a-3b4c-5d6e7f8g9h0i' then answer_value_text end) as "f6a7b8c9-d0e1-4f2a-3b4c-5d6e7f8g9h0i",max(case when linkid = 'fe874736-efd1-4fba-b517-5560489729b0' then answer_value_text end) as "fe874736-efd1-4fba-b517-5560489729b0",max(case when linkid = 'financial-hardships-list' then answer_value_text end) as "financial-hardships-list",max(case when linkid = 'is-fws' then answer_value_text end) as "is-fws",max(case when linkid = 'is-ipc' then answer_value_text end) as "is-ipc",max(case when linkid = 'is-ipc-main' then answer_value_text end) as "is-ipc-main",max(case when linkid = 'is-ipc-optional' then answer_value_text end) as "is-ipc-optional",max(case when linkid = 'is-mhs' then answer_value_text end) as "is-mhs",max(case when linkid = 'is-other-agency-services' then answer_value_text end) as "is-other-agency-services",max(case when linkid = 'is-other-services-outside-agency' then answer_value_text end) as "is-other-services-outside-agency",max(case when linkid = 'is-sbirt' then answer_value_text end) as "is-sbirt",max(case when linkid = 'is-sbirt-main' then answer_value_text end) as "is-sbirt-main",max(case when linkid = 'is-sbirt-optional' then answer_value_text end) as "is-sbirt-optional",max(case when linkid = 'is-spi' then answer_value_text end) as "is-spi",max(case when linkid = 'patient-age' then answer_value_text end) as "patient-age",max(case when linkid = 'patient-dob' then answer_value_text end) as "patient-dob",max(case when linkid = 'patient-name' then answer_value_text end) as "patient-name",max(case when linkid = 'scheduled-ipc-session-1-with-current-time' then answer_value_text end) as "scheduled-ipc-session-1-with-current-time",max(case when linkid = 'scheduled-ipc-session-1-with-current-time-2' then answer_value_text end) as "scheduled-ipc-session-1-with-current-time-2",max(case when linkid = 'task-id-000' then answer_value_text end) as "task-id-000",max(case when linkid = 'task-id-001' then answer_value_text end) as "task-id-001",max(case when linkid = 'task-id-002' then answer_value_text end) as "task-id-002",max(case when linkid = 'task-id-003' then answer_value_text end) as "task-id-003",max(case when linkid = 'task-id-004' then answer_value_text end) as "task-id-004",max(case when linkid = 'task-id-006' then answer_value_text end) as "task-id-006",max(case when linkid = 'task-id-planning-next-steps-pdf' then answer_value_text end) as "task-id-planning-next-steps-pdf"
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

  
  

