
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_mood_rating_s1_readable__dbt_tmp"
    
    
  as (
    -- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"

-- Mood Rating (IPC Session 1) with readable column names
-- Questionnaire ID: 203
-- Source file: questionnaire/ipc-session-1/mood-rating-ipc-session-1.json




  
  

  
  

  
  

  
  
    
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
  

  
  

  with ids(ident) as ( values('Questionnaire/203') ),
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
      b.qr_id,max(case when linkid = 'f1.33.6' then answer_value_text end) as "mood_s1_on_a_scale_of_1_to_10_with_1_being_the_worst_mood_",max(case when linkid = '8085cded-8e0f-497f-9e3d-5fab3981d727' then answer_value_text end) as "mood_s1_8085cded8e0f497f9e3d5fab3981d727",max(case when linkid = 'd58c8907-06c6-4071-aa6c-b2b95d2169c7' then answer_value_text end) as "mood_s1_total_score",max(case when linkid = 'b5bc7f80-4a0c-486c-e5eb-32c750036f94' then answer_value_text end) as "b5bc7f80-4a0c-486c-e5eb-32c750036f94",max(case when linkid = 'calculated-month' then answer_value_text end) as "calculated-month",max(case when linkid = 'calculated-year' then answer_value_text end) as "calculated-year",max(case when linkid = 'cd8e3d6d-e9ff-458d-d122-57070bebffaf' then answer_value_text end) as "cd8e3d6d-e9ff-458d-d122-57070bebffaf",max(case when linkid = 'LINK_ID_THAT_CONTAINS_ENCOUNTER_ID_AS_ANSWER' then answer_value_text end) as "LINK_ID_THAT_CONTAINS_ENCOUNTER_ID_AS_ANSWER"
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