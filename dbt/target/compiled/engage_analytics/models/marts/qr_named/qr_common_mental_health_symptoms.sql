-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"

-- Common Mental Health Symptoms with readable column names
-- Questionnaire ID: q-common-mental-health-symptoms
-- Source file: questionnaire/assessments/common-mental-health-symptoms.json




  
  

  
  

  
  

  
  
    
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
  

  
  

  with ids(ident) as ( values('Questionnaire/q-common-mental-health-symptoms') ),
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
      b.qr_id,max(case when linkid = '16d05294-fee9-4d42-8447-0d578a5de192' then answer_value_text end) as "place_declare_values",max(case when linkid = '347352fc-b189-444a-aa22-df4d9e779f67' then answer_value_text end) as "short_form_pcl",max(case when linkid = '4a05b4fe-b514-4751-91be-adb1b278430c' then answer_value_text end) as "place_declare_values_3",max(case when linkid = '532fcdbc-38b7-4a35-870c-1d5222e239fb' then answer_value_text end) as "short_form_pcl_4",max(case when linkid = '583c5b02-b26c-4821-8da8-7ed9066d9d5d' then answer_value_text end) as "place_declare_values_5",max(case when linkid = '5bc16477-41ff-485f-a73f-f0caa9c2c0ab' then answer_value_text end) as "short_form_pcl_6",max(case when linkid = '68bec639-6c70-41e0-88f8-fef0ad941b55' then answer_value_text end) as "short_form_pcl_7",max(case when linkid = '6a52200c-3856-4fcc-afe4-b75921da0122' then answer_value_text end) as "short_form_pcl_8",max(case when linkid = '829e6d32-3fa7-4d16-909b-b198aea54603' then answer_value_text end) as "place_declare_values_9",max(case when linkid = '9c366bcc-38c8-449f-8823-8a1632f44da3' then answer_value_text end) as "problem_difficulty",max(case when linkid = 'b3d8e9fc-0435-47c5-a3f7-a0ce6cf41879' then answer_value_text end) as "place_declare_values_11",max(case when linkid = 'b4f73ad2-ed7a-4268-9057-a4a72b50dd10' then answer_value_text end) as "short_form_pcl_12",max(case when linkid = 'd4b3bbcc-6d23-45e1-9755-1aa0bfc1b646' then answer_value_text end) as "place_declare_values_13",max(case when linkid = 'e13d2bb9-20a6-4605-a33f-5d9047f6b6a8' then answer_value_text end) as "short_form_pcl_14",max(case when linkid = 'eb42041c-cb74-4d5e-8a29-941776cc0b6e' then answer_value_text end) as "short_form_pcl_15",max(case when linkid = 'f1.10.1' then answer_value_text end) as "psychomotor",max(case when linkid = 'f1.11.1' then answer_value_text end) as "suicidal_thoughts",max(case when linkid = 'f1.15.1' then answer_value_text end) as "feeling_nervous",max(case when linkid = 'f1.16.1' then answer_value_text end) as "gad_ipc_session",max(case when linkid = 'f1.17.1' then answer_value_text end) as "excessive_worry",max(case when linkid = 'f1.18.1' then answer_value_text end) as "trouble_relaxing",max(case when linkid = 'f1.19.1' then answer_value_text end) as "restlessness",max(case when linkid = 'f1.20.1' then answer_value_text end) as "irritability",max(case when linkid = 'f1.21.1' then answer_value_text end) as "feeling_afraid",max(case when linkid = 'f1.3.1' then answer_value_text end) as "little_interest",max(case when linkid = 'f1.4.1' then answer_value_text end) as "feeling_down",max(case when linkid = 'f1.5.1' then answer_value_text end) as "sleep_trouble",max(case when linkid = 'f1.6.1' then answer_value_text end) as "low_energy",max(case when linkid = 'f1.7.1' then answer_value_text end) as "appetite_problems",max(case when linkid = 'f1.8.1' then answer_value_text end) as "feeling_bad_self",max(case when linkid = 'f1.9.1' then answer_value_text end) as "concentration_trouble",max(case when linkid = 'patient-age' then answer_value_text end) as "patient_age",max(case when linkid = 'patient-dob' then answer_value_text end) as "patient_dob",max(case when linkid = 'patient-name' then answer_value_text end) as "patient_name",max(case when linkid = 'show-pcl-5-to-8-questions' then answer_value_text end) as "show_pcl_questions",max(case when linkid = 'task-id-common-mental-pdf' then answer_value_text end) as "task_common_mental"
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

  
  

