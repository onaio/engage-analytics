
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_phq_9_s3__dbt_tmp"
    
    
  as (
    -- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"

-- PHQ-9 (IPC Session 3) with readable column names
-- Questionnaire ID: 61
-- Source file: questionnaire/ipc-session-3/phq-9-ipc-session-3.json




  
  

  
  

  
  

  
  
    
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
  

  
  

  with ids(ident) as ( values('Questionnaire/61') ),
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
      b.qr_id,max(case when linkid = '45f52042-6802-4d9c-9f86-6578923d0fd1' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_37",max(case when linkid = '4650a511-2184-4b0b-9e43-54f45f4340a9' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_38",max(case when linkid = '55d78cf4-0963-4c44-af44-6004a8abbb3e' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_39",max(case when linkid = 'f1.10.1' then answer_value_text end) as "common_mental_health_symptoms_moving_or_speaking_so_slowly_th_4",max(case when linkid = 'f1.11.1' then answer_value_text end) as "common_mental_health_symptoms_thoughts_that_you_would_be_bett_4",max(case when linkid = 'f1.3.1' then answer_value_text end) as "common_mental_health_symptoms_little_interest_or_pleasure_in__4",max(case when linkid = 'f1.4.1' then answer_value_text end) as "common_mental_health_symptoms_feeling_down_depressed_or_hopel_4",max(case when linkid = 'f1.5.1' then answer_value_text end) as "common_mental_health_symptoms_trouble_falling_or_staying_asle_4",max(case when linkid = 'f1.6.1' then answer_value_text end) as "common_mental_health_symptoms_feeling_tired_or_having_little__4",max(case when linkid = 'f1.7.1' then answer_value_text end) as "common_mental_health_symptoms_poor_appetite_or_overeating_4",max(case when linkid = 'f1.8.1' then answer_value_text end) as "common_mental_health_symptoms_feeling_bad_about_yourself_or_t_4",max(case when linkid = 'f1.9.1' then answer_value_text end) as "common_mental_health_symptoms_trouble_concentrating_on_things_4"
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