
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_sf_pcl_5_s1__dbt_tmp"
    
    
  as (
    -- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"

-- Short-Form PCL-5-8 (IPC Session 1) with readable column names
-- Questionnaire ID: 204
-- Source file: questionnaire/ipc-session-1/sf-pcl-5-ipc-session-1.json




  
  

  
  

  
  

  
  
    
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
  

  
  

  with ids(ident) as ( values('Questionnaire/204') ),
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
      b.qr_id,max(case when linkid = '16d05294-fee9-4d42-8447-0d578a5de192' then answer_value_text end) as "place_declare_values",max(case when linkid = '2171a88a-4e4d-44c4-b6b3-389723e9b505' then answer_value_text end) as "place_declare_values_2",max(case when linkid = '347352fc-b189-444a-aa22-df4d9e779f67' then answer_value_text end) as "short_form_pcl",max(case when linkid = '532fcdbc-38b7-4a35-870c-1d5222e239fb' then answer_value_text end) as "short_form_pcl_4",max(case when linkid = '5bc16477-41ff-485f-a73f-f0caa9c2c0ab' then answer_value_text end) as "short_form_pcl_5",max(case when linkid = '68bec639-6c70-41e0-88f8-fef0ad941b55' then answer_value_text end) as "short_form_pcl_6",max(case when linkid = '6a52200c-3856-4fcc-afe4-b75921da0122' then answer_value_text end) as "short_form_pcl_7",max(case when linkid = '790391ba-cf9f-4ef6-be11-b5a7b468819f' then answer_value_text end) as "pcl_item",max(case when linkid = 'b4f73ad2-ed7a-4268-9057-a4a72b50dd10' then answer_value_text end) as "short_form_pcl_9",max(case when linkid = 'b5bc7f80-4a0c-486c-e5eb-32c750036f94' then answer_value_text end) as "add_family_member",max(case when linkid = 'b7934aa5-17f0-4320-8f65-e7a7f72c5161' then answer_value_text end) as "pcl_item_11",max(case when linkid = 'calculated-month' then answer_value_text end) as "add_family_member_12",max(case when linkid = 'calculated-year' then answer_value_text end) as "add_family_member_13",max(case when linkid = 'cd8e3d6d-e9ff-458d-d122-57070bebffaf' then answer_value_text end) as "add_family_member_14",max(case when linkid = 'd4b3bbcc-6d23-45e1-9755-1aa0bfc1b646' then answer_value_text end) as "place_declare_values_15",max(case when linkid = 'd74aecd2-f0c4-4207-b396-b2c6d780be4a' then answer_value_text end) as "pcl_item_16",max(case when linkid = 'e13d2bb9-20a6-4605-a33f-5d9047f6b6a8' then answer_value_text end) as "short_form_pcl_17",max(case when linkid = 'ea00e468-9c26-4a1c-a0af-1b72923b1e2e' then answer_value_text end) as "place_declare_values_18",max(case when linkid = 'eb42041c-cb74-4d5e-8a29-941776cc0b6e' then answer_value_text end) as "short_form_pcl_19",max(case when linkid = 'encounter-id' then answer_value_text end) as "place_declare_values_20",max(case when linkid = 'f1.26.1' then answer_value_text end) as "form_question",max(case when linkid = 'f1.27.1' then answer_value_text end) as "form_question_22",max(case when linkid = 'f1.28.1' then answer_value_text end) as "form_question_23",max(case when linkid = 'f1.29.1' then answer_value_text end) as "form_question_24",max(case when linkid = 'f1.30.1' then answer_value_text end) as "form_question_25",max(case when linkid = 'is-ptsd' then answer_value_text end) as "place_declare_values_26",max(case when linkid = 'LINK_ID_THAT_CONTAINS_ENCOUNTER_ID_AS_ANSWER' then answer_value_text end) as "LINK_ID_THAT_CONTAINS_ENCOUNTER_ID_AS_ANSWER",max(case when linkid = 'observation-ptsd-score-id' then answer_value_text end) as "place_declare_values_28"
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