
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_sf_pcl_5_s3__dbt_tmp"
    
    
  as (
    -- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"

-- Short-Form PCL-5-8 (IPC Session 3) with readable column names
-- Questionnaire ID: 60
-- Source file: questionnaire/ipc-session-3/sf-pcl-5-ipc-session-3.json




  
  

  
  

  
  

  
  
    
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
  

  
  

  with ids(ident) as ( values('Questionnaire/60') ),
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
      b.qr_id,max(case when linkid = 'encounter-id' then answer_value_text end) as "ipc_s3_encounter_id_of_ptsd_session_3",max(case when linkid = '347352fc-b189-444a-aa22-df4d9e779f67' then answer_value_text end) as "ipc_s3_in_the_past_month_how_much_were_you_bothered_byfee",max(case when linkid = '532fcdbc-38b7-4a35-870c-1d5222e239fb' then answer_value_text end) as "ipc_s3_in_the_past_month_how_much_were_you_bothered_by_re",max(case when linkid = '5bc16477-41ff-485f-a73f-f0caa9c2c0ab' then answer_value_text end) as "ipc_s3_in_the_past_month_how_much_were_you_bothered_bylos",max(case when linkid = '68bec639-6c70-41e0-88f8-fef0ad941b55' then answer_value_text end) as "ipc_s3_in_the_past_month_how_much_were_you_bothered_by_ha",max(case when linkid = '6a52200c-3856-4fcc-afe4-b75921da0122' then answer_value_text end) as "ipc_s3_in_the_past_month_how_much_were_you_bothered_by_av",max(case when linkid = 'b4f73ad2-ed7a-4268-9057-a4a72b50dd10' then answer_value_text end) as "ipc_s3_in_the_past_month_how_much_were_you_bothered_by_fe",max(case when linkid = 'cbad1c80-ab95-4720-ba6a-0eadd718905d' then answer_value_text end) as "ipc_s3_total_score",max(case when linkid = 'e13d2bb9-20a6-4605-a33f-5d9047f6b6a8' then answer_value_text end) as "ipc_s3_in_the_past_month_how_much_were_you_bothered_by_av_9",max(case when linkid = 'eb42041c-cb74-4d5e-8a29-941776cc0b6e' then answer_value_text end) as "ipc_s3_in_the_past_month_how_much_were_you_bothered_byhav",max(case when linkid = 'd4b3bbcc-6d23-45e1-9755-1aa0bfc1b646' then answer_value_text end) as "ipc_s3_score_meaning",max(case when linkid = 'ea00e468-9c26-4a1c-a0af-1b72923b1e2e' then answer_value_text end) as "ipc_s3_score_meaning_12",max(case when linkid = 'is-ptsd' then answer_value_text end) as "ipc_s3_is_ptsd",max(case when linkid = 'a51fba17-fccd-4b65-ada3-5c50a75e5d49' then answer_value_text end) as "sometimes_things_happen_to_people_that_are_unusually_or_espe",max(case when linkid = 'e4c58005-803f-4bc3-adaa-1f84c5824b6e' then answer_value_text end) as "i_am_going_to_list_some_of_these_events_please_tell_me_if_y",max(case when linkid = 'f5002611-caf9-4792-8d10-e7870459ea17' then answer_value_text end) as "likely_has_ptsd",max(case when linkid = 'f1.26.1' then answer_value_text end) as "f1.26.1",max(case when linkid = 'f1.27.1' then answer_value_text end) as "f1.27.1",max(case when linkid = 'f1.28.1' then answer_value_text end) as "f1.28.1",max(case when linkid = 'f1.29.1' then answer_value_text end) as "f1.29.1",max(case when linkid = 'f1.30.1' then answer_value_text end) as "f1.30.1"
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