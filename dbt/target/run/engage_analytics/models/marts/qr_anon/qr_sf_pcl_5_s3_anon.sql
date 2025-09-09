
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_sf_pcl_5_s3_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_sf_pcl_5_s3
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
    short_form_pcl as short_form_pcl,
    short_form_pcl_2 as short_form_pcl_2,
    short_form_pcl_3 as short_form_pcl_3,
    short_form_pcl_4 as short_form_pcl_4,
    short_form_pcl_5 as short_form_pcl_5,
    pcl_item as pcl_item,
    short_form_pcl_7 as short_form_pcl_7,
    place_declare_values as place_declare_values,
    place_declare_values_9 as place_declare_values_9,
    short_form_pcl_10 as short_form_pcl_10,
    pcl_item_11 as pcl_item_11,
    place_declare_values_12 as place_declare_values_12,
    short_form_pcl_13 as short_form_pcl_13,
    place_declare_values_14 as place_declare_values_14,
    form_question as form_question,
    form_question_16 as form_question_16,
    form_question_17 as form_question_17,
    form_question_18 as form_question_18,
    form_question_19 as form_question_19,
    pcl_item_20 as pcl_item_20,
    place_declare_values_21 as place_declare_values_21,
    CURRENT_TIMESTAMP as anonymized_at

from "airbyte"."engage_analytics_engage_analytics_mart"."qr_sf_pcl_5_s3"
  );