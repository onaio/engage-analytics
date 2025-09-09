-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"

-- PHQ-9 (IPC Session 2) with readable column names
-- Questionnaire ID: 57
-- Source file: questionnaire/ipc-session-2/phq-9-ipc-session-2.json




  
  

  
  

  
  

  
  
    
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
  

  
  

  with ids(ident) as ( values('Questionnaire/57') ),
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
      b.qr_id,max(case when linkid = '7239b680-ef7a-4f9f-87ff-a77bf21d96d4' then answer_value_text end) as "phq_place_declare_values",max(case when linkid = '7afb89dc-d53b-4dc3-a185-43dbb4854172' then answer_value_text end) as "phq_place_declare_values_2",max(case when linkid = 'a07491bd-039d-417b-9caf-0af536316594' then answer_value_text end) as "phq_place_declare_values_3",max(case when linkid = 'encounter-id' then answer_value_text end) as "phq_place_declare_values_4",max(case when linkid = 'f1.10.1' then answer_value_text end) as "psychomotor",max(case when linkid = 'f1.11.1' then answer_value_text end) as "suicidal_thoughts",max(case when linkid = 'f1.3.1' then answer_value_text end) as "little_interest",max(case when linkid = 'f1.4.1' then answer_value_text end) as "feeling_down",max(case when linkid = 'f1.5.1' then answer_value_text end) as "sleep_trouble",max(case when linkid = 'f1.6.1' then answer_value_text end) as "low_energy",max(case when linkid = 'f1.7.1' then answer_value_text end) as "appetite_problems",max(case when linkid = 'f1.8.1' then answer_value_text end) as "feeling_bad_self",max(case when linkid = 'f1.9.1' then answer_value_text end) as "concentration_trouble",max(case when linkid = 'observation-phq-score-id' then answer_value_text end) as "phq_place_declare_values_14"
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

  
  

