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
      b.qr_id,max(case when linkid = '21234567-89ab-cdef-0123-456789abcdef' then answer_value_text end) as "financial_wellness_tool_in_the_past_12_months_was_there_a_time_",max(case when linkid = '31234567-89ab-cdef-0123-456789abcdef' then answer_value_text end) as "financial_wellness_tool_in_the_past_12_months_did_you_not_pay_t",max(case when linkid = '3456789a-bcde-f012-3456-789abcdef012' then answer_value_text end) as "financial_wellness_tool_how_hard_is_it_for_you_to_pay_for_the_v",max(case when linkid = '51234567-89ab-cdef-0123-456789abcdef' then answer_value_text end) as "financial_wellness_tool_how_strongly_do_you_agree_or_disagree_w",max(case when linkid = '61234567-89ab-cdef-0123-456789abcdef' then answer_value_text end) as "financial_wellness_tool_in_the_past_12_months_did_you_move_in_w",max(case when linkid = '6789abcd-ef01-2345-6789-abcdef012345' then answer_value_text end) as "financial_wellness_tool_in_the_past_12_months_how_often_did_you",max(case when linkid = '91234567-89ab-cdef-0123-456789abcdef' then answer_value_text end) as "financial_wellness_tool_in_the_past_12_months_did_you_stay_at_a",max(case when linkid = 'c1234567-89ab-cdef-0123-456789abcdef' then answer_value_text end) as "financial_wellness_tool_in_the_past_12_months_did_you_not_pay_2",max(case when linkid = 'cdef0123-4567-89ab-cdef-0123456789ab' then answer_value_text end) as "financial_wellness_tool_i_worried_whether_my_food_would_run_out",max(case when linkid = 'f0123456-789a-bcde-f012-3456789abcde' then answer_value_text end) as "financial_wellness_tool_the_food_that_i_bought_just_didn_t_last",max(case when linkid = 'f1234567-89ab-cdef-0123-456789abcdef' then answer_value_text end) as "financial_wellness_tool_in_the_past_12_months_was_your_phone_ga",max(case when linkid = 'f1a2b3c4-5e6f-4a1b-8c7d-9e0f1a2b3c4d' then answer_value_text end) as "financial_wellness_tool_when_you_think_about_your_financial_sit",max(case when linkid = 'high-debt' then answer_value_text end) as "financial_wellness_tool",max(case when linkid = 'high-food-insecurity' then answer_value_text end) as "financial_wellness_tool_2",max(case when linkid = 'high-housing-insecurity' then answer_value_text end) as "financial_wellness_tool_3",max(case when linkid = 'high-paying-bills' then answer_value_text end) as "financial_wellness_tool_4",max(case when linkid = 'moderate-debt' then answer_value_text end) as "financial_wellness_tool_5",max(case when linkid = 'moderate-food-insecurity' then answer_value_text end) as "financial_wellness_tool_6",max(case when linkid = 'moderate-housing-insecurity' then answer_value_text end) as "financial_wellness_tool_7",max(case when linkid = 'moderate-medical-hardships' then answer_value_text end) as "financial_wellness_tool_8",max(case when linkid = 'moderate-paying-bills' then answer_value_text end) as "financial_wellness_tool_9",max(case when linkid = 'no-financial-hardship' then answer_value_text end) as "financial_wellness_tool_10",max(case when linkid = 'overall-financial-hardship' then answer_value_text end) as "financial_wellness_tool_11"
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

  
  

