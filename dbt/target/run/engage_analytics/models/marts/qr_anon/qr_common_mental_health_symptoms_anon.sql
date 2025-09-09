
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_common_mental_health_symptoms_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_common_mental_health_symptoms
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
    short_form_pcl as short_form_pcl,
    place_declare_values_3 as place_declare_values_3,
    short_form_pcl_4 as short_form_pcl_4,
    place_declare_values_5 as place_declare_values_5,
    short_form_pcl_6 as short_form_pcl_6,
    short_form_pcl_7 as short_form_pcl_7,
    short_form_pcl_8 as short_form_pcl_8,
    place_declare_values_9 as place_declare_values_9,
    problem_difficulty as problem_difficulty,
    place_declare_values_11 as place_declare_values_11,
    short_form_pcl_12 as short_form_pcl_12,
    place_declare_values_13 as place_declare_values_13,
    short_form_pcl_14 as short_form_pcl_14,
    short_form_pcl_15 as short_form_pcl_15,
    psychomotor as psychomotor,
    suicidal_thoughts as suicidal_thoughts,
    feeling_nervous as feeling_nervous,
    gad_ipc_session as gad_ipc_session,
    excessive_worry as excessive_worry,
    trouble_relaxing as trouble_relaxing,
    restlessness as restlessness,
    irritability as irritability,
    feeling_afraid as feeling_afraid,
    little_interest as little_interest,
    feeling_down as feeling_down,
    sleep_trouble as sleep_trouble,
    low_energy as low_energy,
    appetite_problems as appetite_problems,
    feeling_bad_self as feeling_bad_self,
    concentration_trouble as concentration_trouble,
    'REDACTED' as patient_age,
    'REDACTED' as patient_dob,
    'REDACTED' as patient_name,
    show_pcl_questions as show_pcl_questions,
    task_common_mental as task_common_mental,
    CURRENT_TIMESTAMP as anonymized_at

from "airbyte"."engage_analytics_engage_analytics_mart"."qr_common_mental_health_symptoms"
  );