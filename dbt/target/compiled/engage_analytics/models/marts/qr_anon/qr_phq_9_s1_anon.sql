

-- Anonymized view for qr_phq_9_s1
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
    problem_difficulty as problem_difficulty,
    phq_place_declare_values_4 as phq_place_declare_values_4,
    phq_add_family_member as phq_add_family_member,
    phq_add_family_member_6 as phq_add_family_member_6,
    phq_add_family_member_7 as phq_add_family_member_7,
    phq_add_family_member_8 as phq_add_family_member_8,
    phq_place_declare_values_9 as phq_place_declare_values_9,
    psychomotor as psychomotor,
    suicidal_thoughts as suicidal_thoughts,
    little_interest as little_interest,
    feeling_down as feeling_down,
    sleep_trouble as sleep_trouble,
    low_energy as low_energy,
    appetite_problems as appetite_problems,
    feeling_bad_self as feeling_bad_self,
    concentration_trouble as concentration_trouble,
    phq_place_declare_values_19 as phq_place_declare_values_19,
    phq_place_declare_values_21 as phq_place_declare_values_21,
    CURRENT_TIMESTAMP as anonymized_at

from "airbyte"."engage_analytics_engage_analytics_mart"."qr_phq_9_s1"