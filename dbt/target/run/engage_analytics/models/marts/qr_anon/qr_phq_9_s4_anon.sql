
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_phq_9_s4_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_phq_9_s4
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
    phq_place_declare_values as phq_place_declare_values,
    phq_place_declare_values_2 as phq_place_declare_values_2,
    phq_place_declare_values_3 as phq_place_declare_values_3,
    problem_difficulty as problem_difficulty,
    psychomotor as psychomotor,
    suicidal_thoughts as suicidal_thoughts,
    little_interest as little_interest,
    feeling_down as feeling_down,
    sleep_trouble as sleep_trouble,
    low_energy as low_energy,
    appetite_problems as appetite_problems,
    feeling_bad_self as feeling_bad_self,
    concentration_trouble as concentration_trouble,
    CURRENT_TIMESTAMP as anonymized_at

from "airbyte"."engage_analytics_engage_analytics_mart"."qr_phq_9_s4"
  );