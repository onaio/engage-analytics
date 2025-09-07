
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_gad7_s3_readable__dbt_tmp"
    
    
  as (
    -- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"

-- GAD-7 (IPC Session 3) with readable column names
-- Questionnaire ID: 59




  
  

  
  

  
  

  
  
    
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
  

  
  

  with ids(ident) as ( values('Questionnaire/59') ),
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
      b.qr_id,max(case when linkid = 'f1.15.1' then answer_value_text end) as "ipc_s3_q1_anxious",max(case when linkid = 'f1.16.1' then answer_value_text end) as "ipc_s3_q2_control_worry",max(case when linkid = 'f1.17.1' then answer_value_text end) as "ipc_s3_q3_worry_too_much",max(case when linkid = 'f1.18.1' then answer_value_text end) as "ipc_s3_q4_trouble_relaxing",max(case when linkid = 'f1.19.1' then answer_value_text end) as "ipc_s3_q5_restless",max(case when linkid = 'f1.20.1' then answer_value_text end) as "ipc_s3_q6_irritable",max(case when linkid = 'f1.21.1' then answer_value_text end) as "ipc_s3_q7_afraid",max(case when linkid = '534c4998-b964-4101-b919-ff3c26af9714' then answer_value_text end) as "ipc_s3_total_score",max(case when linkid = 'd34c5839-fad9-4824-b2dc-9e0161647402' then answer_value_text end) as "ipc_s3_score_meaning",max(case when linkid = '3212b2b2-f42f-46e9-b6bd-42fc29671da9' then answer_value_text end) as "ipc_s3_score_meaning_10"
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