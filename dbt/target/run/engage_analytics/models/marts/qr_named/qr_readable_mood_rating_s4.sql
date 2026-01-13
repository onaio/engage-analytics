
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_readable_mood_rating_s4__dbt_tmp"
    
    
  as (
    -- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"

-- Mood Rating (IPC Session 4) with readable column names
-- Questionnaire ID: 209
-- Source file: questionnaire/ipc-session-4/mood-rating-ipc-session-4.json




  
  

  
  

  
  

  
  
    
    
      
    
      
    
  

  
  

  with ids(ident) as ( values('Questionnaire/209') ),
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
      b.qr_id,max(case when linkid = 'f1.33.6' then answer_value_text end) as "mood_s4_on_a_scale_of_1_to_10_with_1_being_the_worst_mood_",max(case when linkid = 'c04706e8-4a1e-4e6a-ac6e-be9dd8274990' then answer_value_text end) as "mood_s4_total_score"
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