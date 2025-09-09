-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header"
-- depends_on: "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"

-- SBIRT with readable column names
-- Questionnaire ID: 212
-- Source file: questionnaire/sbirt.json




  
  

  
  

  
  

  
  
    
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
      
    
  

  
  

  with ids(ident) as ( values('Questionnaire/212') ),
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
      b.qr_id,max(case when linkid = '0466b304-2876-409b-ade2-d1c675614854' then answer_value_text end) as "sbirt_please_select_one",max(case when linkid = '092f7cb4-cfcc-4b1b-ab5d-b7f2d68fc671' then answer_value_text end) as "sbirt_average_many_days",max(case when linkid = '0eb07ad0-c8fa-49b6-9c27-064d844b6c4f' then answer_value_text end) as "sbirt_cocaine_coke_crack",max(case when linkid = '15799969-7565-4c3e-b488-d6a65d66c6c9' then answer_value_text end) as "sbirt_sbirt",max(case when linkid = '1600f791-fd2c-44d8-8384-37973785741b' then answer_value_text end) as "sbirt_three_you_failed",max(case when linkid = '19d44e35-f7b5-43e3-be3b-fdb820503f78' then answer_value_text end) as "like_offer_opportunity",max(case when linkid = '1bc8823a-bbb1-444a-ad1b-dc09f57038ce' then answer_value_text end) as "friend_relative_anyone",max(case when linkid = '1eb189a2-2ae1-4a27-b4cf-9161447ff8b4' then answer_value_text end) as "sbirt_you_ever_tried",max(case when linkid = '21a9c779-27be-4c08-b9ab-b0d84964145a' then answer_value_text end) as "sbirt_three_you_failed_9",max(case when linkid = '232a2454-1d23-4ec9-b4ee-65e3a48afb3e' then answer_value_text end) as "sbirt_average_many_days_10",max(case when linkid = '2336b352-cd6b-4044-8879-a5425c4ae5cb' then answer_value_text end) as "sbirt_three_you_strong",max(case when linkid = '272246d4-b9c5-4a8b-a349-12f75cf888d0' then answer_value_text end) as "start_discussing_pros",max(case when linkid = '287b1b6c-fe58-4245-90dc-87d72d296812' then answer_value_text end) as "sbirt_things_you",max(case when linkid = '28edfdea-ec6f-4d33-80e2-6d7323b3b515' then answer_value_text end) as "sbirt_you_ever_tried_14",max(case when linkid = '2b8c1391-ba62-4800-ab3d-287fd7a9a3a5' then answer_value_text end) as "sbirt_three_you_strong_15",max(case when linkid = '2badbdc1-2d13-4368-b803-c2eb7a2647aa' then answer_value_text end) as "sbirt_three_you_strong_16",max(case when linkid = '36af2335-ac86-4864-8a3d-619451160cc5' then answer_value_text end) as "sbirt_survey_response",max(case when linkid = '380d715b-abd0-4808-9ec4-0a1aabf9a33f' then answer_value_text end) as "sbirt_survey_response_18",max(case when linkid = '386eb911-a4c5-4207-9dab-8d5973069ac5' then answer_value_text end) as "sbirt_survey_response_19",max(case when linkid = '38eb7cfd-8f4a-4743-b856-c8ddd529dac3' then answer_value_text end) as "prescription_stimulants_just",max(case when linkid = '39843731-a8e7-4822-bad7-55489947ebeb' then answer_value_text end) as "sbirt_average_many_days_21",max(case when linkid = '3b2fcef7-2e63-4aaf-b08e-c16dbc5bc33f' then answer_value_text end) as "hallucinogens_lsd_acid",max(case when linkid = '3e1d29fe-7774-4731-bf50-ce5faaf0a528' then answer_value_text end) as "sbirt_survey_response_23",max(case when linkid = '43dead8d-380d-4214-80f1-a1961b916bd3' then answer_value_text end) as "sbirt_three_your_use",max(case when linkid = '445adcc0-9c5f-4647-b339-fcb5835f1730' then answer_value_text end) as "sbirt_survey_response_25",max(case when linkid = '48d61a4b-d87d-4661-96dd-42766dbe8cc3' then answer_value_text end) as "you_interested_learning",max(case when linkid = '49bafbbf-8110-46a0-83f4-802cb1b6fd0f' then answer_value_text end) as "sbirt_three_your_use_27",max(case when linkid = '49ee0853-7d07-40af-a139-7cacc302ee41' then answer_value_text end) as "sbirt_three_your_use_28",max(case when linkid = '4e2b0891-5355-4ffd-8a18-66193a0dbfbe' then answer_value_text end) as "sbirt_three_you_used",max(case when linkid = '54f4cac5-ac52-4caa-a6b9-f8b4a412d92e' then answer_value_text end) as "sbirt_you_ever_tried_30",max(case when linkid = '582271e0-892e-4795-ac55-0b73baf828ca' then answer_value_text end) as "friend_relative_anyone_31",max(case when linkid = '590f26f6-d5e5-4b56-b3a8-415e038dc045' then answer_value_text end) as "sbirt_survey_response_32",max(case when linkid = '5de2f3d9-de95-4871-919e-a00bb4f346bb' then answer_value_text end) as "sbirt_survey_response_33",max(case when linkid = '5fa54c70-1483-4dfa-92fc-22ec4d90593c' then answer_value_text end) as "friend_relative_anyone_34",max(case when linkid = '6281fa0d-ef3f-4013-bd31-062371828010' then answer_value_text end) as "inhalants_nitrous_glue",max(case when linkid = '642b120d-78f5-4ec6-9cbf-d3b9e336083e' then answer_value_text end) as "sbirt_you_someone_else",max(case when linkid = '654d5688-f42a-469b-8494-81115b760e29' then answer_value_text end) as "friend_relative_anyone_37",max(case when linkid = '67c70e9e-2e10-4d4a-b354-95621a05c621' then answer_value_text end) as "sbirt_you_six_more",max(case when linkid = '687cad76-ce92-4dca-b94f-58070b33da6f' then answer_value_text end) as "sbirt_average_many_days_39",max(case when linkid = '6ad8b269-9415-4551-8d00-9be8812bde01' then answer_value_text end) as "sbirt_three_your_use_40",max(case when linkid = '6c1d97a4-bb16-4c46-bcee-5593f472b53b' then answer_value_text end) as "sbirt_last_you_needed",max(case when linkid = '6c5e1af3-63d6-4283-9ea3-ef2b49314b1a' then answer_value_text end) as "sbirt_three_you_strong_42",max(case when linkid = '6f1dfe02-2dbb-4371-ab06-a8c915495069' then answer_value_text end) as "sbirt_average_many_days_43",max(case when linkid = '6fb3e9da-948e-4d05-8cad-58d293f557f6' then answer_value_text end) as "sbirt_survey_response_44",max(case when linkid = '705b4c26-4479-4d6a-adbe-b8ebdbad302e' then answer_value_text end) as "sbirt_three_you_failed_45",max(case when linkid = '71679b91-ee7b-4eda-9f9d-cf85645ee848' then answer_value_text end) as "you_ever_experienced",max(case when linkid = '724d0558-550c-4edc-a1d6-3bd1285bc8a5' then answer_value_text end) as "sbirt_survey_response_47",max(case when linkid = '73b23b0b-ff20-4eb9-aebb-a34432897116' then answer_value_text end) as "friend_relative_anyone_48",max(case when linkid = '7baaf5ce-f19b-4a40-980e-cfe53efac3f3' then answer_value_text end) as "sbirt_average_many_days_49",max(case when linkid = '7ee9b164-9830-4f7a-8317-28e0cfc1852c' then answer_value_text end) as "sbirt_three_your_use_50",max(case when linkid = '7f8ade46-1d91-40a0-b67e-015a5a6aadc3' then answer_value_text end) as "sbirt_survey_response_51",max(case when linkid = '818b51b8-c7b3-4d64-8142-1c0798662985' then answer_value_text end) as "sbirt_three_you_strong_52",max(case when linkid = '8427af19-3bad-4713-bad9-b179437fef96' then answer_value_text end) as "sbirt_average_many_days_53",max(case when linkid = '859d58d6-ee56-4ede-9773-0b7650d7c66d' then answer_value_text end) as "sbirt_survey_response_54",max(case when linkid = '86daa888-2174-4fdc-9200-cbfdbe430ab4' then answer_value_text end) as "you_blackouts_flashbacks",max(case when linkid = '88accd91-b2ff-4bba-a5b1-58eaef458474' then answer_value_text end) as "sbirt_three_you_strong_56",max(case when linkid = '893ab0b5-f2ce-4fe2-8e2a-f9bc2a010b04' then answer_value_text end) as "sbirt_three_you_used_57",max(case when linkid = '8a6c4c5b-2fc5-46f7-b9e8-e7f3865a0427' then answer_value_text end) as "sbirt_survey_response_58",max(case when linkid = '8bcb58a6-5720-4746-b0d0-22d6b6ec7c96' then answer_value_text end) as "sbirt_you_ever_tried_59",max(case when linkid = '90876967-c973-4bee-9217-8c7482cb12a2' then answer_value_text end) as "sbirt_three_you_strong_60",max(case when linkid = '9346e7e0-e74b-452a-addc-e6ad13d016dd' then answer_value_text end) as "sbirt_last_you_feeling",max(case when linkid = '94dc51b7-59e3-4276-9afe-9b18cb7a08e1' then answer_value_text end) as "many_standard_drinks",max(case when linkid = '958d0ff5-edd9-4d25-9f4d-4ff29289bbe2' then answer_value_text end) as "sbirt_three_you_failed_63",max(case when linkid = '9724b66b-2162-4984-b352-0bc27bcfdfef' then answer_value_text end) as "sbirt_last_you_unable",max(case when linkid = '9cfbf571-09a3-4a3d-ae9d-24cfdf0b9ce3' then answer_value_text end) as "sbirt_some_good_things",max(case when linkid = '9d8d7688-d1ed-4851-a6ea-59bc296f5368' then answer_value_text end) as "cannabis_marijuana_pot",max(case when linkid = '9dfe8a90-6551-4ae4-bcfb-c839e429a87b' then answer_value_text end) as "sbirt_average_many_days_67",max(case when linkid = '9f1fb719-c235-4826-a53e-24502bc620cc' then answer_value_text end) as "sbirt_you_ever_tried_68",max(case when linkid = 'a204f61e-3c3c-4cb1-9e3c-78cf8ea7ad5a' then answer_value_text end) as "friend_relative_anyone_69",max(case when linkid = 'a36058d0-3ac9-4cf6-aabb-6b4859dc985d' then answer_value_text end) as "sbirt_three_your_use_70",max(case when linkid = 'a44b78b7-d5ab-49a8-958d-5545072692d8' then answer_value_text end) as "sbirt_average_many_days_71",max(case when linkid = 'a6d0780b-f202-416d-8d9b-c418a4bdb5d0' then answer_value_text end) as "sbirt_you_ever_tried_72",max(case when linkid = 'a713e390-f199-4ed4-9a27-b9938fbf8b11' then answer_value_text end) as "you_drink_containing",max(case when linkid = 'a7443184-d141-4bce-93fc-c393132c73b1' then answer_value_text end) as "friend_relative_anyone_74",max(case when linkid = 'aa01eb31-553b-415c-bb90-1ec8d07e979e' then answer_value_text end) as "friend_relative_anyone_75",max(case when linkid = 'aa901327-bb55-4e04-a72b-5bcc7eb22404' then answer_value_text end) as "sbirt_any_other_drugs",max(case when linkid = 'ab8f95f3-3ca8-439f-afc2-c95a0853f9f4' then answer_value_text end) as "sbirt_you_neglected_your",max(case when linkid = 'abb64c97-d3a9-45a2-bec2-11f6c4dc8848' then answer_value_text end) as "street_opioids_heroin",max(case when linkid = 'ad609c63-366e-476a-b131-610ceed211da' then answer_value_text end) as "sbirt_things_others_help",max(case when linkid = 'ae15db19-8d0c-465a-a925-3e0503f0a25b' then answer_value_text end) as "sbirt_three_your_use_80",max(case when linkid = 'af4330a4-7bc7-48db-bce8-364b0ff4b041' then answer_value_text end) as "you_interested_learning_81",max(case when linkid = 'b11bfd65-1f8d-4dee-8a9e-b2044382c0d5' then answer_value_text end) as "sbirt_three_you_strong_82",max(case when linkid = 'b5bc7f80-4a0c-486c-e5eb-32c750036f94' then answer_value_text end) as "sbirt_add_family_member",max(case when linkid = 'b81b6e43-3b57-489e-8ea9-f35d82f9f267' then answer_value_text end) as "sbirt_you_ever_feel",max(case when linkid = 'b87d6054-9f94-456f-b7f6-67c73be8acc7' then answer_value_text end) as "sbirt_survey_response_85",max(case when linkid = 'ba10b935-7861-4ce4-a2c6-88bdd97709fb' then answer_value_text end) as "friend_relative_anyone_86",max(case when linkid = 'ba444442-73b2-44be-b55e-c8f9fba8f7a8' then answer_value_text end) as "sbirt_three_you_failed_87",max(case when linkid = 'c08dae42-df65-4dba-98c6-47f9e5dafcb0' then answer_value_text end) as "sbirt_three_you_used_88",max(case when linkid = 'c0911201-29b6-4cff-8cf8-fca31dc2b990' then answer_value_text end) as "you_medical_problems",max(case when linkid = 'c3e99643-32dc-4e2e-983c-e694159623e3' then answer_value_text end) as "sbirt_you_ever_used",max(case when linkid = 'c5437ab8-5d0c-4157-b06a-b5ec491dc299' then answer_value_text end) as "sbirt_other_drugs",max(case when linkid = 'c6c7bd0c-812d-4931-b94c-929aa04dc45c' then answer_value_text end) as "sbirt_you_engaged_illegal",max(case when linkid = 'c93222e9-84f3-44a3-a87d-d92dafdc558e' then answer_value_text end) as "sbirt_three_you_used_93",max(case when linkid = 'calculated-month' then answer_value_text end) as "sbirt_add_family_member_94",max(case when linkid = 'calculated-year' then answer_value_text end) as "sbirt_add_family_member_95",max(case when linkid = 'cd6aa7a8-5c5d-40bd-a096-d3a77a3afc2e' then answer_value_text end) as "sbirt_three_you_used_96",max(case when linkid = 'cd8e3d6d-e9ff-458d-d122-57070bebffaf' then answer_value_text end) as "sbirt_add_family_member_97",max(case when linkid = 'd11e0ac3-5faf-4986-a633-22efa8c74c4d' then answer_value_text end) as "sbirt_three_you_strong_98",max(case when linkid = 'd169ab54-4544-4d1a-8687-6abc289982a4' then answer_value_text end) as "sbirt_last_you_failed",max(case when linkid = 'd4317308-24ec-4ce6-8659-54d4f1aa7e96' then answer_value_text end) as "sbirt_three_your_use_100",max(case when linkid = 'da1016a2-b3c5-4ea5-ae4a-0cce5d9c51db' then answer_value_text end) as "sbirt_three_you_used_101",max(case when linkid = 'da17f7fc-0919-44cb-a41c-fa8480bd3c40' then answer_value_text end) as "sbirt_you_ever_tried_102",max(case when linkid = 'da46a37c-1678-4f34-80b5-f69589bc8844' then answer_value_text end) as "sbirt_three_you_failed_103",max(case when linkid = 'daf9b1ff-06c5-45a7-a3d1-26977a2e2b41' then answer_value_text end) as "sbirt_your_spouse_parents",max(case when linkid = 'db4b99b9-2cae-4833-bb08-fe7e94156b49' then answer_value_text end) as "sbirt_three_you_used_105",max(case when linkid = 'db7cc8b8-b558-41c8-9849-a80252710491' then answer_value_text end) as "sbirt_scale_being_not",max(case when linkid = 'dc9d1ab7-8f17-4ea3-b9c1-24ff00a5f197' then answer_value_text end) as "sbirt_three_you_used_107",max(case when linkid = 'ddb86794-a4d0-47e8-8a6d-2d27b8e4c4f4' then answer_value_text end) as "sbirt_survey_response_108",max(case when linkid = 'drug-to-use-alcohol' then answer_value_text end) as "sbirt_drug_use_alcohol",max(case when linkid = 'drug-to-use-marijuana-or-other' then answer_value_text end) as "sbirt_drug_use_marijuana",max(case when linkid = 'e06c266d-8bd5-4232-85dc-f61a9d33aff2' then answer_value_text end) as "sbirt_three_your_use_111",max(case when linkid = 'e1d9ecfa-c456-4109-8807-b8e26ffb69aa' then answer_value_text end) as "sbirt_three_you_failed_112",max(case when linkid = 'e2c84a83-1568-4593-9ea4-4f6c3978edd6' then answer_value_text end) as "sbirt_three_you_failed_113",max(case when linkid = 'e4bd2450-08cb-410f-8f81-743d84749347' then answer_value_text end) as "sbirt_three_you_used_114",max(case when linkid = 'e5787dda-4b43-4ae0-8cea-3062732f5a9d' then answer_value_text end) as "you_interested_learning_115",max(case when linkid = 'e8e7d8b0-88e2-4bae-8990-c3ae94165ef9' then answer_value_text end) as "sbirt_you_always_able",max(case when linkid = 'e9a44d1a-ecf7-43af-835c-f4b5f73828d3' then answer_value_text end) as "sbirt_survey_response_117",max(case when linkid = 'ea3c9480-429c-4f5c-ac95-0c8c68b8c4b0' then answer_value_text end) as "sbirt_average_many_days_118",max(case when linkid = 'eaa83f10-5cad-4c71-96b9-528e45af44fe' then answer_value_text end) as "sedatives_just_feeling",max(case when linkid = 'ebf2e364-e7d7-44ec-a463-757c8e24d509' then answer_value_text end) as "sbirt_some_good_things_120",max(case when linkid = 'ec346c72-9835-4b5a-8088-1797a200f995' then answer_value_text end) as "sbirt_last_you_found",max(case when linkid = 'edced328-6f82-458a-bce4-35987bff19fb' then answer_value_text end) as "sbirt_sbirt_122",max(case when linkid = 'ef06e3fc-45f0-40a3-aca9-43293339689b' then answer_value_text end) as "sbirt_you_used_drugs",max(case when linkid = 'f53db683-31e1-4d83-82c4-7e3a02f9ee88' then answer_value_text end) as "relative_friend_doctor",max(case when linkid = 'f7444114-6b17-4300-80a1-a6376fd135d4' then answer_value_text end) as "sbirt_three_you_failed_125",max(case when linkid = 'f7c474fa-87f7-454c-9f1b-1f53b9e4d1ac' then answer_value_text end) as "methamphetamine_meth_crystal",max(case when linkid = 'f9fc5555-b1e6-4251-8047-13162c0b0cca' then answer_value_text end) as "sbirt_you_ever_tried_127",max(case when linkid = 'fac09a6e-c280-426d-94a1-0aa707e35084' then answer_value_text end) as "sbirt_you_ever_tried_128",max(case when linkid = 'fae920b4-84d9-4576-9fac-51cba163ffd1' then answer_value_text end) as "sbirt_your_health_goals",max(case when linkid = 'fd89df57-4b13-432f-a5b9-d54918f8d4ba' then answer_value_text end) as "sbirt_you_use_more",max(case when linkid = 'ff55dd63-f51c-420b-97a0-f957d31d6b19' then answer_value_text end) as "share_additional_treatment",max(case when linkid = 'health-goals-do-not-want-to-change' then answer_value_text end) as "sbirt_health_goals_not",max(case when linkid = 'is-drug-dependence' then answer_value_text end) as "sbirt_drug_dependence",max(case when linkid = 'is-high-risk-alcohol' then answer_value_text end) as "sbirt_high_risk_alcohol",max(case when linkid = 'is-high-risk-drug' then answer_value_text end) as "sbirt_high_risk_drug",max(case when linkid = 'is-low-risk-alcohol' then answer_value_text end) as "sbirt_low_risk_alcohol",max(case when linkid = 'is-low-risk-drug' then answer_value_text end) as "sbirt_low_risk_drug",max(case when linkid = 'is-medium-risk-alcohol' then answer_value_text end) as "sbirt_medium_risk_alcohol",max(case when linkid = 'is-medium-risk-drug' then answer_value_text end) as "sbirt_medium_risk_drug",max(case when linkid = 'is-opioid-dependence' then answer_value_text end) as "sbirt_opioid_dependence",max(case when linkid = 'other-drug-list' then answer_value_text end) as "sbirt_other_drug_list",max(case when linkid = 'patient-age' then answer_value_text end) as "patient_age",max(case when linkid = 'patient-dob' then answer_value_text end) as "patient_dob",max(case when linkid = 'patient-gender' then answer_value_text end) as "patient_gender",max(case when linkid = 'patient-name' then answer_value_text end) as "patient_name",max(case when linkid = 'score-audit' then answer_value_text end) as "sbirt_score_audit",max(case when linkid = 'score-dast' then answer_value_text end) as "sbirt_score_dast",max(case when linkid = 'should-converse-about-substance-use' then answer_value_text end) as "converse_substance_use",max(case when linkid = 'should-discuss-health-goals' then answer_value_text end) as "discuss_health_goals",max(case when linkid = 'show-client-use-with-other-drugs' then answer_value_text end) as "sbirt_show_client_use",max(case when linkid = 'show-discuss-next-steps' then answer_value_text end) as "sbirt_show_discuss_next",max(case when linkid = 'show-goals' then answer_value_text end) as "sbirt_show_goals",max(case when linkid = 'show-intervention-when-low-risk' then answer_value_text end) as "show_intervention_low",max(case when linkid = 'show-intervention-when-medium-high-risk' then answer_value_text end) as "show_intervention_medium",max(case when linkid = 'show-is-medium-high-risk' then answer_value_text end) as "sbirt_show_medium_high",max(case when linkid = 'show-summary-for-low-risk' then answer_value_text end) as "sbirt_show_summary_low",max(case when linkid = 'show-things-to-do' then answer_value_text end) as "sbirt_show_things",max(case when linkid = 'task-id-sbirt-pdf' then answer_value_text end) as "sbirt_task_sbirt_pdf",max(case when linkid = 'using-opioids-and-low-risk' then answer_value_text end) as "sbirt_using_opioids_low"
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

  
  

