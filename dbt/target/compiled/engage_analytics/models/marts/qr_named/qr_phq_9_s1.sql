-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"

-- PHQ-9 (IPC Session 1) with readable column names
-- Questionnaire ID: 54
-- Source file: questionnaire/ipc-session-1/phq-9-ipc-session-1.json




  
  

  
  

  
  

  
  
    
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
  

  
  

  with ids(ident) as ( values('Questionnaire/54') ),
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
      b.qr_id,max(case when linkid = 'f1.3.1' then answer_value_text end) as "phq9_q1_interest",max(case when linkid = 'f1.4.1' then answer_value_text end) as "phq9_q2_depressed",max(case when linkid = 'f1.5.1' then answer_value_text end) as "phq9_q3_sleep",max(case when linkid = 'f1.6.1' then answer_value_text end) as "phq9_q4_tired",max(case when linkid = 'f1.7.1' then answer_value_text end) as "phq9_q5_appetite",max(case when linkid = 'f1.8.1' then answer_value_text end) as "phq9_q6_failure",max(case when linkid = 'f1.9.1' then answer_value_text end) as "phq9_q7_concentration",max(case when linkid = 'f1.10.1' then answer_value_text end) as "phq9_q8_movement",max(case when linkid = 'f1.11.1' then answer_value_text end) as "phq9_q9_suicide",max(case when linkid = 'b3d8e9fc-0435-47c5-a3f7-a0ce6cf41879' then answer_value_text end) as "phq9_total_score",max(case when linkid = '4a05b4fe-b514-4751-91be-adb1b278430c' then answer_value_text end) as "phq9_score_meaning",max(case when linkid = '0cfe26b9-8cd8-4d03-938f-991ebab4b3c0' then answer_value_text end) as "phq9_score_meaning_code",max(case when linkid = 'encounter-id' then answer_value_text end) as "phq9_encounter_id",max(case when linkid = 'observation-phq-score-id' then answer_value_text end) as "phq9_observation_id",max(case when linkid = 'calculated-month' then answer_value_text end) as "phq9_calculated_month",max(case when linkid = 'calculated-year' then answer_value_text end) as "phq9_calculated_year",max(case when linkid = 'cd8e3d6d-e9ff-458d-d122-57070bebffaf' then answer_value_text end) as "phq9_field_cd8e3d6d",max(case when linkid = 'f5675128-0cd4-423f-840c-8dcff1e2a429' then answer_value_text end) as "phq9_task_id",max(case when linkid = '9c366bcc-38c8-449f-8823-8a1632f44da3' then answer_value_text end) as "phq9_field_9c366bcc",max(case when linkid = 'b5bc7f80-4a0c-486c-e5eb-32c750036f94' then answer_value_text end) as "phq9_field_b5bc7f80",max(case when linkid = 'LINK_ID_THAT_CONTAINS_ENCOUNTER_ID_AS_ANSWER' then answer_value_text end) as "phq9_link_encounter"
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

  
  

