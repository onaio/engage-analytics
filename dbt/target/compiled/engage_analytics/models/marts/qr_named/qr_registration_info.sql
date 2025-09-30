-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"

-- Add Family Member Registration with readable column names
-- Questionnaire ID: 221
-- Source file: questionnaire/generic/registration_info.json




  
  

  
  

  
  

  
  
    
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
  

  
  

  with ids(ident) as ( values('Questionnaire/221') ),
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
      b.qr_id,max(case when linkid = '36d39dbe-00e2-4e29-f079-3ec8f0119aff' then answer_value_text end) as "add_family_member_registration",max(case when linkid = '597f4425-72b3-4bc4-8f38-b742aa3e99cd' then answer_value_text end) as "add_family_member_registration_middle_name",max(case when linkid = '77e32953-0679-48b5-f004-1ab4a4ac0271' then answer_value_text end) as "add_family_member_registration_which_of_the_following_best_repr",max(case when linkid = '8460d986-ef71-4997-80ee-7887d8c345e7' then answer_value_text end) as "add_family_member_registration_age_estimate_if_unknown",max(case when linkid = '8fb87910-d900-4eea-dbba-dcf76ee6806d' then answer_value_text end) as "add_family_member_registration_last_name",max(case when linkid = 'age' then answer_value_text end) as "age",max(case when linkid = 'b5bc7f80-4a0c-486c-e5eb-32c750036f94' then answer_value_text end) as "add_family_member_registration_calculated_age_6",max(case when linkid = 'c02a80e7-ebf0-48b5-9752-28a48dba487f' then answer_value_text end) as "add_family_member_registration_unique_id",max(case when linkid = 'c31ef6d1-336b-4260-9881-67ba7b477ae0' then answer_value_text end) as "add_family_member_registration_have_you_received_engage_service",max(case when linkid = 'calculated-month' then answer_value_text end) as "add_family_member_registration_calculated_month_6",max(case when linkid = 'calculated-year' then answer_value_text end) as "add_family_member_registration_calculated_year_6",max(case when linkid = 'cd8e3d6d-e9ff-458d-d122-57070bebffaf' then answer_value_text end) as "add_family_member_registration_date_of_birth_6",max(case when linkid = 'choice-gender-identity' then answer_value_text end) as "add_family_member_registration_what_term_best_expresses_how_you",max(case when linkid = 'choice-pronouns' then answer_value_text end) as "add_family_member_registration_what_are_your_preferred_pronouns",max(case when linkid = 'do-you-have-medicaid' then answer_value_text end) as "add_family_member_registration_do_you_have_medicaid",max(case when linkid = 'e6306275-b989-4375-8527-3a56092081b8' then answer_value_text end) as "add_family_member_registration_first_name",max(case when linkid = 'email-address' then answer_value_text end) as "add_family_member_registration_email_address",max(case when linkid = 'f9aa068d-8d94-4b3d-b9da-ce05a59cecdd' then answer_value_text end) as "add_family_member_registration_what_was_your_biological_sex_at_",max(case when linkid = 'medicaid-number' then answer_value_text end) as "add_family_member_registration_medicaid_number",max(case when linkid = 'other-best-represents-how-you-think-of-yourself' then answer_value_text end) as "add_family_member_registration_2",max(case when linkid = 'other-biological-sex-at-birth' then answer_value_text end) as "add_family_member_registration_3",max(case when linkid = 'other-gender-identity' then answer_value_text end) as "add_family_member_registration_4",max(case when linkid = 'other-pronouns' then answer_value_text end) as "add_family_member_registration_5",max(case when linkid = 'other-where-did-you-first-learn-about-engage' then answer_value_text end) as "add_family_member_registration_6",max(case when linkid = 'phone-number' then answer_value_text end) as "add_family_member_registration_phone_number",max(case when linkid = 'physical-address' then answer_value_text end) as "add_family_member_registration_physical_address",max(case when linkid = 'where-did-you-first-learn-about-engage' then answer_value_text end) as "add_family_member_registration_where_did_you_first_learn_about_",max(case when linkid = 'zip-code' then answer_value_text end) as "add_family_member_registration_zip_code"
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

  
  

