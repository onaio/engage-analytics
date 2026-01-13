
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_phq9_s3_readable__dbt_tmp"
    
    
  as (
    -- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"

-- PHQ-9 (IPC Session 3) with readable column names
-- Questionnaire ID: 61




  
  

  
  

  
  

  
  
    
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
  

  
  

  with ids(ident) as ( values('Questionnaire/61') ),
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
      b.qr_id,max(case when linkid = '45f52042-6802-4d9c-9f86-6578923d0fd1' then answer_value_text end) as "ipc_s3_total_score",max(case when linkid = 'f1.10.1' then answer_value_text end) as "ipc_s3_q8_movement",max(case when linkid = 'f1.11.1' then answer_value_text end) as "ipc_s3_q9_suicide",max(case when linkid = 'f1.3.1' then answer_value_text end) as "ipc_s3_q1_interest",max(case when linkid = 'f1.4.1' then answer_value_text end) as "ipc_s3_q2_depressed",max(case when linkid = 'f1.5.1' then answer_value_text end) as "ipc_s3_q3_sleep",max(case when linkid = 'f1.6.1' then answer_value_text end) as "ipc_s3_q4_tired",max(case when linkid = 'f1.7.1' then answer_value_text end) as "ipc_s3_q5_appetite",max(case when linkid = 'f1.8.1' then answer_value_text end) as "ipc_s3_q6_failure",max(case when linkid = 'f1.9.1' then answer_value_text end) as "ipc_s3_q7_concentration",max(case when linkid = '55d78cf4-0963-4c44-af44-6004a8abbb3e' then answer_value_text end) as "ipc_s3_score_meaning",max(case when linkid = '4650a511-2184-4b0b-9e43-54f45f4340a9' then answer_value_text end) as "ipc_s3_score_meaning_12"
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