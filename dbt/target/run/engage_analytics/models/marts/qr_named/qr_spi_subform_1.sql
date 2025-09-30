
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_spi_subform_1__dbt_tmp"
    
    
  as (
    -- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"

-- SPI Subform 1 with readable column names
-- Questionnaire ID: 104455
-- Source file: questionnaire/spi/spi-subform-1.json




  
  

  
  

  
  

  
  
    
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
  

  
  

  with ids(ident) as ( values('Questionnaire/104455') ),
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
      b.qr_id,max(case when linkid = '06e33f1f-7202-4d5e-94d1-efbeabaa3e7d' then answer_value_text end) as "spi_subform_1_is_medium_risk",max(case when linkid = '2897fef9-abcf-4bb3-b33f-2accae4c06c5' then answer_value_text end) as "spi_subform_1_were_any_of_these_in_the_past_three_months",max(case when linkid = '35062b98-748b-441c-ad74-185823e1bb52' then answer_value_text end) as "spi_subform_1_in_the_past_month_have_you_been_thinking_about_ho",max(case when linkid = '3f85f2ef-d781-4001-8df5-99a9ac5a656b' then answer_value_text end) as "spi_subform_1_is_high_risk",max(case when linkid = '41aee132-ab8e-4d64-a322-eed88f900edb' then answer_value_text end) as "spi_subform_1_in_the_past_month_have_you_had_these_thoughts_and",max(case when linkid = '43898926-e9d5-400e-a095-62a8c7f8dd03' then answer_value_text end) as "spi_subform_1",max(case when linkid = '567e992d-fb9e-4608-9be1-9277beae3c23' then answer_value_text end) as "spi_subform_1_in_the_past_month_have_you_started_to_work_out_or",max(case when linkid = '644c00f2-8b07-4911-92d0-f05f5e5cc837' then answer_value_text end) as "spi_subform_1_2",max(case when linkid = '71dbaac9-867f-4d7f-b6d7-e1f59402bbf9' then answer_value_text end) as "spi_subform_1_3",max(case when linkid = '7cc2e8a9-197a-42ea-9431-ba01eefc6511' then answer_value_text end) as "spi_subform_1_in_the_past_month_have_you_wished_you_were_dead_o",max(case when linkid = '7fc39fba-84db-490d-af4e-8f772d91ffda' then answer_value_text end) as "spi_subform_1_4",max(case when linkid = '91e9233a-9ab1-410a-b5e3-b30867355cea' then answer_value_text end) as "spi_subform_1_have_you_ever_done_anything_started_to_do_anythin",max(case when linkid = 'a6601f97-0a34-49d0-b181-611d6705ca42' then answer_value_text end) as "spi_subform_1_5",max(case when linkid = 'c7d21a57-0fc6-4e3a-b411-beceb1fb2423' then answer_value_text end) as "spi_subform_1_6",max(case when linkid = 'd60ed1ee-79c9-444f-aff7-d346974426ab' then answer_value_text end) as "spi_subform_1_is_low_risk",max(case when linkid = 'e2a74544-b333-4767-b3e3-7772413419ce' then answer_value_text end) as "spi_subform_1_in_the_past_month_have_you_actually_had_any_thoug",max(case when linkid = 'ecabd308-c45a-41f7-8754-b1f63a101019' then answer_value_text end) as "spi_subform_1_7",max(case when linkid = 'encounter-id' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_65",max(case when linkid = 'task-id-spi-subform-2' then answer_value_text end) as "spi_subform_1_taskid_spi_subform_2",max(case when linkid = 'task-id-spi-subform-3' then answer_value_text end) as "spi_subform_2_taskid_spi_subform_3",max(case when linkid = 'task-id-spi-subform-4' then answer_value_text end) as "spi_subform_2_taskid_spi_subform_4",max(case when linkid = 'task-status-spi-subform-2' then answer_value_text end) as "spi_subform_1_taskstatus_spi_subform_2",max(case when linkid = 'task-status-spi-subform-3' then answer_value_text end) as "spi_subform_2_taskstatus_spi_subform_3",max(case when linkid = 'task-status-spi-subform-4' then answer_value_text end) as "spi_subform_2_taskstatus_spi_subform_4"
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