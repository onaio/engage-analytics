-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"

-- Financial Wellness Tool with readable column names
-- Questionnaire ID: q-financial-wellness-tool
-- Source file: questionnaire/assessments/financial-wellness-tool.json




  
  

  
  

  
  

  
  
    
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
  

  
  

  with ids(ident) as ( values('Questionnaire/q-financial-wellness-tool') ),
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
      b.qr_id,max(case when linkid = '21234567-89ab-cdef-0123-456789abcdef' then answer_value_text end) as "fin_there_you",max(case when linkid = '31234567-89ab-cdef-0123-456789abcdef' then answer_value_text end) as "fin_you_not_pay",max(case when linkid = '3456789a-bcde-f012-3456-789abcdef012' then answer_value_text end) as "pay_basics_difficulty",max(case when linkid = '51234567-89ab-cdef-0123-456789abcdef' then answer_value_text end) as "fin_strongly_you_agree",max(case when linkid = '61234567-89ab-cdef-0123-456789abcdef' then answer_value_text end) as "fin_you_move_other",max(case when linkid = '6789abcd-ef01-2345-6789-abcdef012345' then answer_value_text end) as "fin_you_run_out",max(case when linkid = '91234567-89ab-cdef-0123-456789abcdef' then answer_value_text end) as "shelter_stay",max(case when linkid = 'c1234567-89ab-cdef-0123-456789abcdef' then answer_value_text end) as "fin_you_not_pay_8",max(case when linkid = 'cdef0123-4567-89ab-cdef-0123456789ab' then answer_value_text end) as "food_insecurity",max(case when linkid = 'f0123456-789a-bcde-f012-3456789abcde' then answer_value_text end) as "fin_food_bought_just",max(case when linkid = 'f1234567-89ab-cdef-0123-456789abcdef' then answer_value_text end) as "fin_your_phone_gas",max(case when linkid = 'f1a2b3c4-5e6f-4a1b-8c7d-9e0f1a2b3c4d' then answer_value_text end) as "fin_you_think_your",max(case when linkid = 'high-debt' then answer_value_text end) as "financial_wellness_tool",max(case when linkid = 'high-food-insecurity' then answer_value_text end) as "financial_wellness_tool_14",max(case when linkid = 'high-housing-insecurity' then answer_value_text end) as "financial_wellness_tool_15",max(case when linkid = 'high-paying-bills' then answer_value_text end) as "financial_wellness_tool_16",max(case when linkid = 'moderate-debt' then answer_value_text end) as "financial_wellness_tool_17",max(case when linkid = 'moderate-food-insecurity' then answer_value_text end) as "financial_wellness_tool_18",max(case when linkid = 'moderate-housing-insecurity' then answer_value_text end) as "financial_wellness_tool_19",max(case when linkid = 'moderate-medical-hardships' then answer_value_text end) as "financial_wellness_tool_20",max(case when linkid = 'moderate-paying-bills' then answer_value_text end) as "financial_wellness_tool_21",max(case when linkid = 'no-financial-hardship' then answer_value_text end) as "financial_wellness_tool_22",max(case when linkid = 'overall-financial-hardship' then answer_value_text end) as "financial_wellness_tool_23"
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

  
  

