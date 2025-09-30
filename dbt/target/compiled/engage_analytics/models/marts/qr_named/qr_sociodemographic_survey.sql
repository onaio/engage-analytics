-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"

-- Sociodemographic Survey with readable column names
-- Questionnaire ID: 1613531
-- Source file: questionnaire/assessments/sociodemographic_survey.json




  
  

  
  

  
  

  
  
    
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
  

  
  

  with ids(ident) as ( values('Questionnaire/1613531') ),
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
      b.qr_id,max(case when linkid = '05990b62-11a8-4ab7-8988-1d2a17edcff7' then answer_value_text end) as "sociodemographic_survey_if_you_were_not_born_in_the_us_how_long",max(case when linkid = '0e027468-c1e9-4977-f5f5-1cfa00ab8af5' then answer_value_text end) as "sociodemographic_survey_please_specify",max(case when linkid = '1fdf1a17-7af6-413f-81e9-cf6823fe7fde' then answer_value_text end) as "sociodemographic_survey_please_specify_2",max(case when linkid = '2f8a7973-d9bb-493b-aa68-fb01e0f1aefe' then answer_value_text end) as "sociodemographic_survey_where_were_you_born",max(case when linkid = '3409c1f0-adf5-4b3b-8ea3-e7672e4cb5af' then answer_value_text end) as "demographic_question",max(case when linkid = '3960c72d-b6e7-418e-86e9-f8348a49e739' then answer_value_text end) as "demographic_question_2",max(case when linkid = '4baa9e68-fe7a-489a-8b03-cc045b53d93d' then answer_value_text end) as "sociodemographic_survey_please_enter_the_number_of_years",max(case when linkid = '5b133a13-0303-4d2f-8884-ffa931d2c173' then answer_value_text end) as "sociodemographic_survey_we_would_like_to_know_about_what_you_do",max(case when linkid = '66d30814-73ea-4fa4-edf8-d0d79378b130' then answer_value_text end) as "demographic_question_3",max(case when linkid = '6e621fcc-a25c-4070-8fda-894bb9df68e4' then answer_value_text end) as "sociodemographic_survey_please_enter_the_number_of_months",max(case when linkid = '7b68e9a3-703a-4b57-876e-8dd400826db9' then answer_value_text end) as "sociodemographic_survey_what_is_your_race_and_or_ethnicity",max(case when linkid = '8f600ee9-8d17-43df-ed68-c9a7de2da7d0' then answer_value_text end) as "demographic_question_4",max(case when linkid = '92fed595-49f2-4fab-81ff-885941f31e07' then answer_value_text end) as "demographic_question_5",max(case when linkid = '9cce9788-d7e7-4456-d455-5bd8dc3d2f2a' then answer_value_text end) as "sociodemographic_survey_what_is_your_marital_status",max(case when linkid = 'b4afce83-4279-4d19-8a8b-1e0b7e1ba73c' then answer_value_text end) as "demographic_question_6",max(case when linkid = 'd7e0e5c9-35bd-40bc-9224-9cf9e4b9dbba' then answer_value_text end) as "sociodemographic_survey_what_was_the_highest_grade_or_level_of_",max(case when linkid = 'f11fe547-9be9-4693-8a85-f252e4a6262f' then answer_value_text end) as "sociodemographic_survey_please_specify_3",max(case when linkid = 'patient-age' then answer_value_text end) as "patient_age_6",max(case when linkid = 'patient-biological-sex' then answer_value_text end) as "patient_biological_sex_3",max(case when linkid = 'patient-dob' then answer_value_text end) as "patient_date_of_birth_6",max(case when linkid = 'patient-gender-identity' then answer_value_text end) as "patient_gender_identity_3",max(case when linkid = 'patient-how-you-think-of-yourself' then answer_value_text end) as "patient_how_you_think_of_yourself_3",max(case when linkid = 'patient-name' then answer_value_text end) as "patient_name_6",max(case when linkid = 'patient-pronouns' then answer_value_text end) as "patient_pronouns_3",max(case when linkid = 'task-id-socio-pdf' then answer_value_text end) as "task_id_socio_pdf_2"
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

  
  

