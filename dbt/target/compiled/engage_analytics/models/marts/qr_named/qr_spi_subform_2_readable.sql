-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"

-- SPI Subform 2 with readable column names
-- Questionnaire ID: 104453
-- Source file: questionnaire/spi/spi-subform-2.json




  
  

  
  

  
  

  
  
    
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
  

  
  

  with ids(ident) as ( values('Questionnaire/104453') ),
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
      b.qr_id,max(case when linkid = 'encounter-id' then answer_value_text end) as "spi_encounter_id_of_spi_sub_2",max(case when linkid = '18c6510f-b294-4741-ae43-9cbbc4ba7e36' then answer_value_text end) as "spi_did_this_client_also_screen_positive_for_probable_",max(case when linkid = '27b1e829-9ad9-41c2-8cb5-01ce1f469896' then answer_value_text end) as "spi_did_you_discuss_this_clients_severe_mental_health_",max(case when linkid = '4c804862-f4b9-49df-aae4-c7802e723c76' then answer_value_text end) as "spi_what_is_the_recommended_plan_to_address_the_probab",max(case when linkid = 'ed537539-d317-43fc-a4b3-38de5e15ea4f' then answer_value_text end) as "spi_what_is_the_recommended_plan_to_address_the_probab_5",max(case when linkid = 'task-id-spi-subform-3' then answer_value_text end) as "spi_taskid_spi_subform_3",max(case when linkid = 'd02eb882-411e-4bc0-af9d-08a77fcc3f50' then answer_value_text end) as "spi_d02eb882411e4bc0af9d08a77fcc3f50",max(case when linkid = 'task-status-spi-subform-3' then answer_value_text end) as "spi_taskstatus_spi_subform_3",max(case when linkid = 'task-id-spi-subform-4' then answer_value_text end) as "spi_taskid_spi_subform_4",max(case when linkid = 'task-status-spi-subform-4' then answer_value_text end) as "spi_taskstatus_spi_subform_4",max(case when linkid = '53b30f12-74eb-474c-941c-5f0e81b7eb9f' then answer_value_text end) as "spi_53b30f1274eb474c941c5f0e81b7eb9f",max(case when linkid = 'f4b782e5-18e1-4edf-a3ff-409feb207253' then answer_value_text end) as "spi_did_you_discuss_this_clients_suicide_risk_screenin",max(case when linkid = '2dd6c90f-ea68-45ba-9d30-58e02ab0812b' then answer_value_text end) as "spi_this_client_scored_in_the_moderate_to_high_range_o",max(case when linkid = '783e2646-c8ea-466d-bba8-85c891a14435' then answer_value_text end) as "spi_783e2646c8ea466dbba885c891a14435"
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

  
  

