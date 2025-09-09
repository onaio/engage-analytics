
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_sf_pcl_5_s1_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_sf_pcl_5_s1
-- Automatically generated based on questionnaire_metadata.csv

select 
    questionnaire_id as questionnaire_id,
    subject_patient_id as subject_patient_id,
    encounter_id as encounter_id,
    author_practitioner_id as author_practitioner_id,
    practitioner_location_id as practitioner_location_id,
    practitioner_organization_id as practitioner_organization_id,
    practitioner_id as practitioner_id,
    practitioner_careteam_id as practitioner_careteam_id,
    application_version as application_version,
    qr_id as qr_id,
    place_declare_values as place_declare_values,
    place_declare_values_2 as place_declare_values_2,
    short_form_pcl as short_form_pcl,
    short_form_pcl_4 as short_form_pcl_4,
    short_form_pcl_5 as short_form_pcl_5,
    short_form_pcl_6 as short_form_pcl_6,
    short_form_pcl_7 as short_form_pcl_7,
    pcl_item as pcl_item,
    short_form_pcl_9 as short_form_pcl_9,
    add_family_member as add_family_member,
    pcl_item_11 as pcl_item_11,
    add_family_member_12 as add_family_member_12,
    add_family_member_13 as add_family_member_13,
    add_family_member_14 as add_family_member_14,
    place_declare_values_15 as place_declare_values_15,
    pcl_item_16 as pcl_item_16,
    short_form_pcl_17 as short_form_pcl_17,
    place_declare_values_18 as place_declare_values_18,
    short_form_pcl_19 as short_form_pcl_19,
    place_declare_values_20 as place_declare_values_20,
    form_question as form_question,
    form_question_22 as form_question_22,
    form_question_23 as form_question_23,
    form_question_24 as form_question_24,
    form_question_25 as form_question_25,
    place_declare_values_26 as place_declare_values_26,
    place_declare_values_28 as place_declare_values_28,
    CURRENT_TIMESTAMP as anonymized_at

from "airbyte"."engage_analytics_engage_analytics_mart"."qr_sf_pcl_5_s1"
  );