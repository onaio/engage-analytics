-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"

-- PHQ-9 (IPC Session 4) with readable column names
-- Questionnaire ID: 211




  
  

  
  

  
  

  
  
    
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
  

  
  

  with ids(ident) as ( values('Questionnaire/211') ),
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
      b.qr_id,max(case when linkid = '01a4984f-7b50-47bb-9ae1-1b81ee4d6df2' then answer_value_text end) as "ipc_s4_total_score",max(case when linkid = '9c366bcc-38c8-449f-8823-8a1632f44da3' then answer_value_text end) as "ipc_s4_how_difficult_have_these_problems_made_it_for_you_",max(case when linkid = 'f1.10.1' then answer_value_text end) as "ipc_s4_q8_movement",max(case when linkid = 'f1.11.1' then answer_value_text end) as "ipc_s4_q9_suicide",max(case when linkid = 'f1.3.1' then answer_value_text end) as "ipc_s4_q1_interest",max(case when linkid = 'f1.4.1' then answer_value_text end) as "ipc_s4_q2_depressed",max(case when linkid = 'f1.5.1' then answer_value_text end) as "ipc_s4_q3_sleep",max(case when linkid = 'f1.6.1' then answer_value_text end) as "ipc_s4_q4_tired",max(case when linkid = 'f1.7.1' then answer_value_text end) as "ipc_s4_q5_appetite",max(case when linkid = 'f1.8.1' then answer_value_text end) as "ipc_s4_q6_failure",max(case when linkid = 'f1.9.1' then answer_value_text end) as "ipc_s4_q7_concentration",max(case when linkid = '11d368a4-7f34-46d5-a8d0-6deed381945b' then answer_value_text end) as "ipc_s4_score_meaning",max(case when linkid = '4b86c897-4d68-405b-aa08-760ed6ff6792' then answer_value_text end) as "ipc_s4_score_meaning_13"
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

  
  

