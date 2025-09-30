-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"

-- GAD-7 (IPC Session 4) with readable column names
-- Questionnaire ID: 62
-- Source file: questionnaire/ipc-session-4/gad-7-ipc-session-4.json




  
  

  
  

  
  

  
  
    
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
  

  
  

  with ids(ident) as ( values('Questionnaire/62') ),
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
      b.qr_id,max(case when linkid = '3212b2b2-f42f-46e9-b6bd-42fc29671da9' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_23",max(case when linkid = '3e626219-c0e4-465e-92e6-9e976c52614c' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_24",max(case when linkid = 'e4155e07-1d18-448f-8111-182bf4149672' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_25",max(case when linkid = 'f1.15.1' then answer_value_text end) as "common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_5",max(case when linkid = 'f1.16.1' then answer_value_text end) as "common_mental_health_symptoms_not_being_able_to_stop_or_contr_5",max(case when linkid = 'f1.17.1' then answer_value_text end) as "common_mental_health_symptoms_worrying_too_much_about_differe_5",max(case when linkid = 'f1.18.1' then answer_value_text end) as "common_mental_health_symptoms_trouble_relaxing_5",max(case when linkid = 'f1.19.1' then answer_value_text end) as "common_mental_health_symptoms_being_so_restless_that_it_s_har_5",max(case when linkid = 'f1.20.1' then answer_value_text end) as "common_mental_health_symptoms_becoming_easily_annoyed_or_irri_5",max(case when linkid = 'f1.21.1' then answer_value_text end) as "common_mental_health_symptoms_feeling_afraid_as_if_something__5"
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

  
  

