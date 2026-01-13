-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"

-- GAD-7 (IPC Session 1) with readable column names
-- Questionnaire ID: 202
-- Source file: questionnaire/ipc-session-1/gad-7-ipc-session-1.json




  
  

  
  

  
  

  
  
    
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
  

  
  

  with ids(ident) as ( values('Questionnaire/202') ),
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
      b.qr_id,max(case when linkid = '3212b2b2-f42f-46e9-b6bd-42fc29671da9' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirpa_9",max(case when linkid = '583c5b02-b26c-4821-8da8-7ed9066d9d5d' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_10",max(case when linkid = '69f16284-b8a8-4a24-ab79-03bde5bcfbe9' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_11",max(case when linkid = '829e6d32-3fa7-4d16-909b-b198aea54603' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_12",max(case when linkid = 'b5bc7f80-4a0c-486c-e5eb-32c750036f94' then answer_value_text end) as "add_family_member_registration_calculated_age_2",max(case when linkid = 'calculated-month' then answer_value_text end) as "add_family_member_registration_calculated_month_2",max(case when linkid = 'calculated-year' then answer_value_text end) as "add_family_member_registration_calculated_year_2",max(case when linkid = 'cd8e3d6d-e9ff-458d-d122-57070bebffaf' then answer_value_text end) as "add_family_member_registration_date_of_birth_2",max(case when linkid = 'encounter-id' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_13",max(case when linkid = 'f1.15.1' then answer_value_text end) as "common_mental_health_symptoms_feeling_nervous_anxious_or_on_e_2",max(case when linkid = 'f1.16.1' then answer_value_text end) as "common_mental_health_symptoms_not_being_able_to_stop_or_contr_2",max(case when linkid = 'f1.17.1' then answer_value_text end) as "common_mental_health_symptoms_worrying_too_much_about_differe_2",max(case when linkid = 'f1.18.1' then answer_value_text end) as "common_mental_health_symptoms_trouble_relaxing_2",max(case when linkid = 'f1.19.1' then answer_value_text end) as "common_mental_health_symptoms_being_so_restless_that_it_s_har_2",max(case when linkid = 'f1.20.1' then answer_value_text end) as "common_mental_health_symptoms_becoming_easily_annoyed_or_irri_2",max(case when linkid = 'f1.21.1' then answer_value_text end) as "common_mental_health_symptoms_feeling_afraid_as_if_something__2",max(case when linkid = 'LINK_ID_THAT_CONTAINS_ENCOUNTER_ID_AS_ANSWER' then answer_value_text end) as "LINK_ID_THAT_CONTAINS_ENCOUNTER_ID_AS_ANSWER",max(case when linkid = 'observation-gad-score-id' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_14"
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

  
  

