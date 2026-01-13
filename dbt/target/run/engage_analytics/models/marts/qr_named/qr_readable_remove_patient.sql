
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_readable_remove_patient__dbt_tmp"
    
    
  as (
    -- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"

-- eCBIS Remove Family Form with readable column names
-- Questionnaire ID: 69
-- Source file: questionnaire/generic/remove_patient.json




  
  

  
  

  
  

  
  
    
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
  

  
  

  with ids(ident) as ( values('Questionnaire/69') ),
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
      b.qr_id,max(case when linkid = '17b7fda5-514c-4408-88e7-52313ab0ef2f' then answer_value_text end) as "remove_family_date_of_death",max(case when linkid = '8489d249-1ef3-4edb-aa99-b5713fd2cb0a' then answer_value_text end) as "remove_family_give_other_reasons_for_removal",max(case when linkid = '98ac4694-590e-4cc2-83f3-cc6455e688c5' then answer_value_text end) as "remove_family_age_at_death_year",max(case when linkid = 'a11ebb0d-acb3-4038-956b-293a41acb85b' then answer_value_text end) as "remove_family_a11ebb0dacb34038956b293a41acb85b",max(case when linkid = 'b6c21488-87b8-4213-8cd0-d482367669c1' then answer_value_text end) as "remove_family_date_moved_away",max(case when linkid = 'f83bb7c0-dfd6-4aa8-9cb9-89d48b8040ff' then answer_value_text end) as "remove_family_reason_for_removal",max(case when linkid = 'patient-birthdate' then answer_value_text end) as "remove_family_patientbirthdate",max(case when linkid = 'cd73cfdf-6d70-4b42-8887-21e27db0b9ec' then answer_value_text end) as "cd73cfdf-6d70-4b42-8887-21e27db0b9ec"
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