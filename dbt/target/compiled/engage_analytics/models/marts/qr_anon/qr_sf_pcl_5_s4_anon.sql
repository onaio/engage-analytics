

-- Anonymized view for qr_sf_pcl_5_s4
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
    place_declare_values as place_declare_values,
    short_form_pcl_5 as short_form_pcl_5,
    short_form_pcl_6 as short_form_pcl_6,
    pcl_item as pcl_item,
    short_form_pcl_8 as short_form_pcl_8,
    place_declare_values_9 as place_declare_values_9,
    pcl_item_10 as pcl_item_10,
    pcl_item_11 as pcl_item_11,
    short_form_pcl_12 as short_form_pcl_12,
    place_declare_values_13 as place_declare_values_13,
    short_form_pcl_14 as short_form_pcl_14,
    place_declare_values_15 as place_declare_values_15,
    form_question as form_question,
    form_question_17 as form_question_17,
    form_question_18 as form_question_18,
    form_question_19 as form_question_19,
    form_question_20 as form_question_20,
    CURRENT_TIMESTAMP as anonymized_at

from "airbyte"."engage_analytics_engage_analytics_mart"."qr_sf_pcl_5_s4"