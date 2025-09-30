
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_sf_pcl_5_s4__dbt_tmp"
    
    
  as (
    -- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"

-- Short-Form PCL-5-8 (IPC Session 4) with readable column names
-- Questionnaire ID: 210
-- Source file: questionnaire/ipc-session-4/sf-pcl-5-ipc-session-4.json




  
  

  
  

  
  

  
  
    
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
  

  
  

  with ids(ident) as ( values('Questionnaire/210') ),
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
      b.qr_id,max(case when linkid = '347352fc-b189-444a-aa22-df4d9e779f67' then answer_value_text end) as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__33",max(case when linkid = '532fcdbc-38b7-4a35-870c-1d5222e239fb' then answer_value_text end) as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__34",max(case when linkid = '5bc16477-41ff-485f-a73f-f0caa9c2c0ab' then answer_value_text end) as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__35",max(case when linkid = '62b55d32-de8e-4abd-8489-4e3aa9270e11' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_61",max(case when linkid = '68bec639-6c70-41e0-88f8-fef0ad941b55' then answer_value_text end) as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__36",max(case when linkid = '6a52200c-3856-4fcc-afe4-b75921da0122' then answer_value_text end) as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__37",max(case when linkid = 'a4b58f6e-3046-4a11-9e9e-e6b1547594e8' then answer_value_text end) as "pcl_5_item_10",max(case when linkid = 'b4f73ad2-ed7a-4268-9057-a4a72b50dd10' then answer_value_text end) as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__38",max(case when linkid = 'd4b3bbcc-6d23-45e1-9755-1aa0bfc1b646' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_62",max(case when linkid = 'd590116a-ec42-46e3-8ab6-6259307af17e' then answer_value_text end) as "pcl_5_item_11",max(case when linkid = 'd5a448fb-4db7-45e8-858b-f2d2e7cb0eef' then answer_value_text end) as "pcl_5_item_12",max(case when linkid = 'e13d2bb9-20a6-4605-a33f-5d9047f6b6a8' then answer_value_text end) as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__39",max(case when linkid = 'ea00e468-9c26-4a1c-a0af-1b72923b1e2e' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_63",max(case when linkid = 'eb42041c-cb74-4d5e-8a29-941776cc0b6e' then answer_value_text end) as "short_form_pcl_5_8_ipc_session_2_in_the_past_month_how_much__40",max(case when linkid = 'encounter-id' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_64",max(case when linkid = 'f1.26.1' then answer_value_text end) as "form_f1_question_26_4",max(case when linkid = 'f1.27.1' then answer_value_text end) as "form_f1_question_27_4",max(case when linkid = 'f1.28.1' then answer_value_text end) as "form_f1_question_28_4",max(case when linkid = 'f1.29.1' then answer_value_text end) as "form_f1_question_29_4",max(case when linkid = 'f1.30.1' then answer_value_text end) as "form_f1_question_30_4"
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