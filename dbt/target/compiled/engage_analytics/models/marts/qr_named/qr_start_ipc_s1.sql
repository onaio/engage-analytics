-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"

-- IPC Session 1 with readable column names
-- Questionnaire ID: 55
-- Source file: questionnaire/ipc-session-1/start-ipc-ipc-session-1.json




  
  

  
  

  
  

  
  
    
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
  

  
  

  with ids(ident) as ( values('Questionnaire/55') ),
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
      b.qr_id,max(case when linkid = '042e5415-93de-419d-9bac-632596939280' then answer_value_text end) as "person_2",max(case when linkid = '0627ed93-c89d-48ae-bde2-bd013dc58685' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_69",max(case when linkid = '07b722c9-210e-491a-8933-2e9aab3474af' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_70",max(case when linkid = '0c99c32b-a6a7-4de6-9fbc-d34ccf133bbf' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_71",max(case when linkid = '0de42ce5-6d9f-40ec-b67b-3d3b975d363b' then answer_value_text end) as "ipc_session_1_what_about_before_that_was_there_a_time_before_in",max(case when linkid = '10151d1b-3cd2-4f10-b1e5-d6b20112d801' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_72",max(case when linkid = '13d72f63-dd18-41f9-bf46-44981f2f804a' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_73",max(case when linkid = '16333904-aad5-4166-9593-b7286c7b20b2' then answer_value_text end) as "ipc_session_1_earlier_than_that_you_noticed_symptoms",max(case when linkid = '1809cb03-235d-4855-b201-7b296b160e37' then answer_value_text end) as "ipc_session_1_is_there_anything_else_that_we_did_not_discuss_th",max(case when linkid = '19416221-7c67-44d7-9b9b-b88364ea7408' then answer_value_text end) as "ipc_session_1",max(case when linkid = '1ae11d83-35d4-41c0-a804-5ddf6b1c388c' then answer_value_text end) as "ipc_session_1_possible_problem_areas",max(case when linkid = '24f9ed3b-3706-43fb-9779-07a9c2077e9d' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_74",max(case when linkid = '31a9e716-52ce-4651-bb17-26f442e2ab89' then answer_value_text end) as "ipc_session_1_what_were_the_symptoms",max(case when linkid = '347d89f5-1bad-4ba5-8f45-e63972b02123' then answer_value_text end) as "ipc_session_1_2",max(case when linkid = '37a6a71f-e6ee-4b2e-bfba-2c2cd51e508b' then answer_value_text end) as "ipc_session_1_3",max(case when linkid = '47eaf38f-c74e-484a-af95-b0002074bf67' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_75",max(case when linkid = '4a2ac26a-8d30-4c28-be11-f684f415f661' then answer_value_text end) as "survey_response_9",max(case when linkid = '528fa2c7-7a88-4d4e-b2ba-38c1384aface' then answer_value_text end) as "ipc_session_1_your_most_distressing_current_symptoms_began",max(case when linkid = '54a97eb0-3fd7-4b2d-b2b7-ea3fe97a1690' then answer_value_text end) as "ipc_session_1_when_did_these_symptoms_begin",max(case when linkid = '5d23d8f0-b23a-4e2f-ac66-497d9c6ab3fd' then answer_value_text end) as "ipc_session_1_when_did_you_notice_these_symptoms",max(case when linkid = '5e65c0dc-5524-4f3d-8404-a7427a34e61c' then answer_value_text end) as "ipc_session_1_these_are_the_symptoms_you_said_you_are_currently",max(case when linkid = '6099951a-c965-40f9-ad61-66e7a39a6b92' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_76",max(case when linkid = '6409c231-50c1-45fc-808e-f0bd015db97d' then answer_value_text end) as "person_3",max(case when linkid = '69125945-bec7-4810-985a-8c1052e8d039' then answer_value_text end) as "ipc_session_1_patient_age",max(case when linkid = '69ed5884-0814-4ae0-a7f1-ecf730710523' then answer_value_text end) as "ipc_session_1_4",max(case when linkid = '78bc8272-eba1-48a8-a2f4-0e47304887b8' then answer_value_text end) as "ipc_session_1_i_noticed_that_when_i_was_asking_about_important_",max(case when linkid = '798d2c43-5b9f-436f-9fa2-f360cbeeea73' then answer_value_text end) as "ipc_session_1_patient_dob",max(case when linkid = '7b0a751e-936e-40e8-9076-c7fe1e818c1f' then answer_value_text end) as "ipc_session_1_5",max(case when linkid = '80344998-41e2-4669-a77e-1fdaebe88bcf' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_77",max(case when linkid = '8085cded-8e0f-497f-9e3d-5fab3981d727' then answer_value_text end) as "mood_rating_ipc_session_1_2",max(case when linkid = '82cdd7f0-f3d1-4eba-a31e-c6ad367742f4' then answer_value_text end) as "ipc_session_1_6",max(case when linkid = '8a8b5e1e-fa6b-472b-ade7-ee8f5286a759' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_78",max(case when linkid = 'a11ebb0d-acb3-4038-956b-293a41acb85b-1' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_79",max(case when linkid = 'a11ebb0d-acb3-4038-956b-293a41acb85b-2' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_80",max(case when linkid = 'a88cb2bf-e23b-423b-9a22-ab94678ccc3a' then answer_value_text end) as "ipc_session_1_do_you_agree_with_this_summary",max(case when linkid = 'acd9d89b-0d67-415e-a3a4-b132ab304c45' then answer_value_text end) as "ipc_session_1_in_what_format_did_you_deliver_this_session_with_",max(case when linkid = 'b1b6bebb-0cb7-4029-b320-01d3270ee892' then answer_value_text end) as "ipc_session_1_7",max(case when linkid = 'b322e52d-99f3-48e0-bda0-09d95228ae7b' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_81",max(case when linkid = 'b3e46a84-9743-4506-9465-43fa16460e16' then answer_value_text end) as "ipc_session_2_did_you_complete_the_required_assessment_s_and_th",max(case when linkid = 'b5bc7f80-4a0c-486c-e5eb-32c750036f94' then answer_value_text end) as "add_family_member_registration_calculated_age_10",max(case when linkid = 'b7b59772-0272-41f6-ba66-16b09a832fc1' then answer_value_text end) as "ipc_session_1_is_there_anything_that_could_get_in_the_way_of_yo",max(case when linkid = 'bd785881-7e04-4093-b191-d845edea4c9d' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_82",max(case when linkid = 'c4b93137-7abc-47f1-8c0a-fbfe8ad6ce2d' then answer_value_text end) as "ipc_session_1_what_was_going_on_in_your_life_when_you_started_t",max(case when linkid = 'calculated-month' then answer_value_text end) as "add_family_member_registration_calculated_month_10",max(case when linkid = 'calculated-year' then answer_value_text end) as "add_family_member_registration_calculated_year_10",max(case when linkid = 'cd69a7a1-ec17-4f43-8597-26c59732261b' then answer_value_text end) as "ipc_session_1_8",max(case when linkid = 'cd8e3d6d-e9ff-458d-d122-57070bebffaf' then answer_value_text end) as "add_family_member_registration_date_of_birth_10",max(case when linkid = 'd126d866-b8c4-4288-84ff-49a40f20e204' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_83",max(case when linkid = 'd58c8907-06c6-4071-aa6c-b2b95d2169c7' then answer_value_text end) as "mood_rating_ipc_session_1_total_score_2",max(case when linkid = 'd6c7419a-9082-4579-8678-44e866ab3655' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_84",max(case when linkid = 'e2629251-1266-4760-a539-f0e680df651b' then answer_value_text end) as "ipc_session_1_what_was_happening_then",max(case when linkid = 'e4ecfbd3-ed37-4b1b-9284-f7cc9386f7b8' then answer_value_text end) as "ipc_session_1_9",max(case when linkid = 'e5899d3e-d8fc-4931-91c8-383f51474a5b' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_85",max(case when linkid = 'e8bfb690-b135-4b0f-9291-f6247611ce52' then answer_value_text end) as "ipc_session_1_of_these_symptoms_which_are_the_most_distressing",max(case when linkid = 'encounter-id' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_86",max(case when linkid = 'f11ace93-5d84-4cc5-8598-58f9c5623037' then answer_value_text end) as "ipc_session_1_at_that_time_you_were_experiencing",max(case when linkid = 'f1.33.6' then answer_value_text end) as "mood_rating_ipc_session_2_on_a_scale_of_1_to_10_with_1_being__5",max(case when linkid = 'f1.38.3' then answer_value_text end) as "form_f1_question_38",max(case when linkid = 'f1.38.4' then answer_value_text end) as "ipc_session_1_10",max(case when linkid = 'f1.39.3.1' then answer_value_text end) as "person_1",max(case when linkid = 'f1.39.3.2' then answer_value_text end) as "person_1_2",max(case when linkid = 'f1.39.3.3' then answer_value_text end) as "person_1_how_has_this_relationship_changed_following_trauma_if_",max(case when linkid = 'f1.39.3.4' then answer_value_text end) as "person_1_person_1_is",max(case when linkid = 'f1.39.4.1' then answer_value_text end) as "person_2_2",max(case when linkid = 'f1.39.4.2' then answer_value_text end) as "person_2_3",max(case when linkid = 'f1.39.4.3' then answer_value_text end) as "person_2_how_has_this_relationship_changed_following_trauma_if_",max(case when linkid = 'f1.39.4.4' then answer_value_text end) as "person_2_person_2_is",max(case when linkid = 'f1.39.5.1' then answer_value_text end) as "person_3_2",max(case when linkid = 'f1.39.5.2' then answer_value_text end) as "person_3_3",max(case when linkid = 'f1.39.5.3' then answer_value_text end) as "person_3_how_has_this_relationship_changed_following_trauma_if_",max(case when linkid = 'f1.39.5.4' then answer_value_text end) as "person_3_person_3_is",max(case when linkid = 'f1.39.6.1' then answer_value_text end) as "person_4",max(case when linkid = 'f1.39.6.2' then answer_value_text end) as "person_4_2",max(case when linkid = 'f1.39.6.3' then answer_value_text end) as "person_4_how_has_this_relationship_changed_following_trauma_if_",max(case when linkid = 'f1.39.6.4' then answer_value_text end) as "person_4_person_4_is",max(case when linkid = 'f1.40.11' then answer_value_text end) as "ipc_session_1_11",max(case when linkid = 'f1.40.12' then answer_value_text end) as "ipc_session_1_has_this_person_been_consistently_close_to_you_or",max(case when linkid = 'f1.40.2' then answer_value_text end) as "ipc_session_1_12",max(case when linkid = 'f1.40.3' then answer_value_text end) as "ipc_session_1_has_this_person_been_consistently_close_to_you__2",max(case when linkid = 'f1.40.5' then answer_value_text end) as "ipc_session_1_13",max(case when linkid = 'f1.40.6' then answer_value_text end) as "ipc_session_1_has_this_person_been_consistently_close_to_you__3",max(case when linkid = 'f1.40.8' then answer_value_text end) as "ipc_session_1_14",max(case when linkid = 'f1.40.9' then answer_value_text end) as "ipc_session_1_has_this_person_been_consistently_close_to_you__4",max(case when linkid = 'f1.42.2' then answer_value_text end) as "ipc_session_1_do_you_think_an_important_person_s_death_is_conne",max(case when linkid = 'f1.43.2' then answer_value_text end) as "ipc_session_1_tell_me_about_your_relationship_with_this_person_",max(case when linkid = 'f1.46.2' then answer_value_text end) as "form_f1_question_46",max(case when linkid = 'f1.68.2' then answer_value_text end) as "ipc_session_1_are_you_and_someone_else_having_a_strong_disagree",max(case when linkid = 'f1.69.2' then answer_value_text end) as "ipc_session_1_briefly_tell_me_about_the_disagreement",max(case when linkid = 'f1.69.3' then answer_value_text end) as "ipc_session_1_when_did_this_disagreement_happen",max(case when linkid = 'f1.70.2' then answer_value_text end) as "ipc_session_1_how_has_this_disagreement_been_affecting_you",max(case when linkid = 'f1.72.2' then answer_value_text end) as "ipc_session_1_have_there_been_any_changes_in_your_life_that_are",max(case when linkid = 'f1.73.2' then answer_value_text end) as "ipc_session_1_what_was_the_change",max(case when linkid = 'f1.74.2' then answer_value_text end) as "ipc_session_1_when_did_this_happen",max(case when linkid = 'f1.76.2' then answer_value_text end) as "ipc_session_1_if_the_client_did_not_mention_any_close_relations",max(case when linkid = 'f1.78.2' then answer_value_text end) as "ipc_session_1_would_you_say_you_have_close_relationships",max(case when linkid = 'f1.79.2' then answer_value_text end) as "ipc_session_1_has_anything_changed_in_your_life_so_that_now_you",max(case when linkid = 'f1.79.3' then answer_value_text end) as "ipc_session_1_when_did_this_event_happen",max(case when linkid = 'f1.81.3' then answer_value_text end) as "ipc_session_1_15",max(case when linkid = 'f1.81.4' then answer_value_text end) as "ipc_session_1_16",max(case when linkid = 'f1.82.2' then answer_value_text end) as "ipc_session_1_do_you_want_to_propose_an_interpersonal_formulati",max(case when linkid = 'f1.87.2' then answer_value_text end) as "ipc_session_1_do_you_agree_with_this",max(case when linkid = 'f1.88.2' then answer_value_text end) as "ipc_session_1_17",max(case when linkid = 'f1.89.2' then answer_value_text end) as "ipc_session_1_select_the_possible_interpersonal_problem_area_s_",max(case when linkid = 'f1.92.1' then answer_value_text end) as "ipc_session_1_18",max(case when linkid = 'fe689023-f019-483b-ba7c-8ff0e16129d5' then answer_value_text end) as "ipc_session_1_at_that_time_you_were_experiencing_2",max(case when linkid = 'LINK_ID_THAT_CONTAINS_ENCOUNTER_ID_AS_ANSWER' then answer_value_text end) as "LINK_ID_THAT_CONTAINS_ENCOUNTER_ID_AS_ANSWER",max(case when linkid = 'observation-mood-score-id' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_87",max(case when linkid = 'task-id-008' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_88",max(case when linkid = 'task-id-009' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_89",max(case when linkid = 'task-id-010' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_90",max(case when linkid = 'task-id-012' then answer_value_text end) as "a_place_to_declare_values_that_cannot_be_created_using_fhirp_91"
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

  
  

