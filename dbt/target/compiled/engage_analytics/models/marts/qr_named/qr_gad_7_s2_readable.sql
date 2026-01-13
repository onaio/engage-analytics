-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"

-- GAD-7 (IPC Session 2) with readable column names
-- Questionnaire ID: 205
-- Source file: questionnaire/ipc-session-2/gad-7-ipc-session-2.json




  
  

  
  

  
  

  
  
    
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
  

  
  

  with ids(ident) as ( values('Questionnaire/205') ),
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
      b.qr_id,max(case when linkid = 'encounter-id' then answer_value_text end) as "ipc_s2_encounter_id_of_gad7_session_2",max(case when linkid = 'f1.15.1' then answer_value_text end) as "ipc_s2_q1_anxious",max(case when linkid = 'f1.16.1' then answer_value_text end) as "ipc_s2_q2_control_worry",max(case when linkid = 'f1.17.1' then answer_value_text end) as "ipc_s2_q3_worry_too_much",max(case when linkid = 'f1.18.1' then answer_value_text end) as "ipc_s2_q4_trouble_relaxing",max(case when linkid = 'f1.19.1' then answer_value_text end) as "ipc_s2_q5_restless",max(case when linkid = 'f1.20.1' then answer_value_text end) as "ipc_s2_q6_irritable",max(case when linkid = 'f1.21.1' then answer_value_text end) as "ipc_s2_q7_afraid",max(case when linkid = 'observation-gad-score-id' then answer_value_text end) as "ipc_s2_observation_id_of_gad7_session_2_score",max(case when linkid = '37e0bcfd-f6d1-4b35-9556-4d458a717a23' then answer_value_text end) as "ipc_s2_total_score",max(case when linkid = '510776d6-8b4e-446b-abe7-80aee8ecb110' then answer_value_text end) as "ipc_s2_score_meaning",max(case when linkid = '3212b2b2-f42f-46e9-b6bd-42fc29671da9' then answer_value_text end) as "ipc_s2_score_meaning_12"
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

  
  

