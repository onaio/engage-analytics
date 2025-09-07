
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_gad_7_s4__dbt_tmp"
    
    
  as (
    -- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"

-- GAD-7 (IPC Session 4) with readable column names
-- Questionnaire ID: 62
-- Source file: questionnaire/ipc-session-4/gad-7-ipc-session-4.json




  
  

  
  

  
  

  
  
    
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
  

  
  

  with ids(ident) as ( values('Questionnaire/62') ),
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
      b.qr_id,max(case when linkid = 'f1.15.1' then answer_value_text end) as "ipc_s4_q1_anxious",max(case when linkid = 'f1.16.1' then answer_value_text end) as "ipc_s4_q2_control_worry",max(case when linkid = 'f1.17.1' then answer_value_text end) as "ipc_s4_q3_worry_too_much",max(case when linkid = 'f1.18.1' then answer_value_text end) as "ipc_s4_q4_trouble_relaxing",max(case when linkid = 'f1.19.1' then answer_value_text end) as "ipc_s4_q5_restless",max(case when linkid = 'f1.20.1' then answer_value_text end) as "ipc_s4_q6_irritable",max(case when linkid = 'f1.21.1' then answer_value_text end) as "ipc_s4_q7_afraid",max(case when linkid = '3e626219-c0e4-465e-92e6-9e976c52614c' then answer_value_text end) as "ipc_s4_total_score",max(case when linkid = 'e4155e07-1d18-448f-8111-182bf4149672' then answer_value_text end) as "ipc_s4_score_meaning",max(case when linkid = '3212b2b2-f42f-46e9-b6bd-42fc29671da9' then answer_value_text end) as "ipc_s4_score_meaning_10"
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