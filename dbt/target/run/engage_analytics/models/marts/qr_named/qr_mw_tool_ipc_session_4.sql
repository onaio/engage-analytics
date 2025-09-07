
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_mw_tool_ipc_session_4__dbt_tmp"
    
    
  as (
    -- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"

-- Mental Wellness Tool Session 4 with readable column names
-- Questionnaire ID: mw-tool-ipc-session-4
-- Source file: questionnaire/assessments/mw-tool-ipc-session-4.json




  
  

  
  

  
  

  
  
    
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
  

  
  

  with ids(ident) as ( values('Questionnaire/mw-tool-ipc-session-4') ),
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
      b.qr_id,max(case when linkid = '0ee3228d-28b6-4c88-8119-808fc121aaa6' then answer_value_text end) as "in_the_past_year_have_there_been_times_when_you_felt_that_s",max(case when linkid = '11e90302-2ae0-4e7d-be11-754b2a91ab20' then answer_value_text end) as "in_the_past_year_have_you_ever_felt_that_your_thoughts_were",max(case when linkid = '156fce37-203f-4760-a9f9-1b8ab0ae0642' then answer_value_text end) as "in_the_past_year_how_often_do_you_have_a_drink_containing_a",max(case when linkid = '2c50fee4-7a5d-4d62-80aa-73329934d7dc' then answer_value_text end) as "in_the_past_month_have_you_wished_you_were_dead_or_wished_y",max(case when linkid = '364e4271-e89d-4206-85a1-45ad595e4cab' then answer_value_text end) as "in_the_past_year_did_you_at_any_time_hear_voices_saying_qui",max(case when linkid = '59ae2c65-3a25-4acb-9db5-7aa035c54a63' then answer_value_text end) as "in_the_last_2_weeks_how_often_have_you_been_feeling_down_d",max(case when linkid = '69a559db-c97b-4bcb-aae4-81465fdd1d20' then answer_value_text end) as "in_the_past_year_how_many_times_have_you_used_a_recreationa",max(case when linkid = '94f913ce-e837-48b3-906d-a0613ac72b73' then answer_value_text end) as "in_the_past_year_how_many_drinks_containing_alcohol_do_you",max(case when linkid = '97f2ed48-11d2-4417-a4b2-57a333c5157f' then answer_value_text end) as "in_the_past_3_months_have_you_ever_done_anything_started_t",max(case when linkid = '99f0d10e-9522-457f-8432-fd489c53acb3' then answer_value_text end) as "in_the_last_2_weeks_how_often_have_you_been_feeling_nervous",max(case when linkid = 'a669eaea-2f12-49ff-ac91-d54009d6ab77' then answer_value_text end) as "in_the_past_month_have_you_had_any_actual_thoughts_of_killi",max(case when linkid = 'b9ecb840-8515-4091-8cef-863ed297012e' then answer_value_text end) as "in_the_last_2_weeks_how_often_have_you_been_so_restless_tha",max(case when linkid = 'f35bbaa2-5e2e-4c74-b9b4-f0aa1eabe47d' then answer_value_text end) as "in_the_past_year_have_there_been_times_when_you_felt_that_a",max(case when linkid = 'alcohol-problem' then answer_value_text end) as "alcohol-problem",max(case when linkid = 'common-mental-health' then answer_value_text end) as "common-mental-health",max(case when linkid = 'drug-problem' then answer_value_text end) as "drug-problem",max(case when linkid = 'how-many-drinks' then answer_value_text end) as "how-many-drinks",max(case when linkid = 'no-mental-problem' then answer_value_text end) as "no-mental-problem",max(case when linkid = 'patient-age' then answer_value_text end) as "patient-age",max(case when linkid = 'patient-biological-sex' then answer_value_text end) as "patient-biological-sex",max(case when linkid = 'patient-dob' then answer_value_text end) as "patient-dob",max(case when linkid = 'patient-gender-identity' then answer_value_text end) as "patient-gender-identity",max(case when linkid = 'patient-how-you-think-of-yourself' then answer_value_text end) as "patient-how-you-think-of-yourself",max(case when linkid = 'patient-name' then answer_value_text end) as "patient-name",max(case when linkid = 'patient-pronouns' then answer_value_text end) as "patient-pronouns",max(case when linkid = 'severe-mental-health' then answer_value_text end) as "severe-mental-health",max(case when linkid = 'suicide-risk' then answer_value_text end) as "suicide-risk",max(case when linkid = 'task-id-mw-tool-ipc-s4-pdf' then answer_value_text end) as "task-id-mw-tool-ipc-s4-pdf"
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