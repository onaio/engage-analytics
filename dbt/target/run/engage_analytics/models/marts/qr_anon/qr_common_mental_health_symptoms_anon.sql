
  create view "airbyte"."engage_analytics_engage_analytics_mart"."qr_common_mental_health_symptoms_anon__dbt_tmp"
    
    
  as (
    

-- Anonymized view for qr_common_mental_health_symptoms with PII fields masked based on questionnaire_metadata.anon flag
-- Auto-generated to mask fields marked as anon=TRUE in questionnaire_metadata

with metadata_pii as (
    select distinct
        question_alias
    from "airbyte"."engage_analytics_engage_analytics_stg"."questionnaire_metadata"
    where questionnaire_id in ('Questionnaire/q-common-mental-health-symptoms')
    and anon = 'TRUE'
),

source_data as (
    select * from "airbyte"."engage_analytics_engage_analytics_mart"."qr_common_mental_health_symptoms"
)

select 
    MD5(COALESCE(qr_id, '')::text) as qr_id_hash,
    questionnaire_id,
    MD5(COALESCE(subject_patient_id, '')::text) as subject_patient_id_hash,
    encounter_id,
    author_practitioner_id,
    practitioner_location_id,
    practitioner_organization_id,
    practitioner_id,
    practitioner_careteam_id,
    application_version,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'total_score')
        THEN 'REDACTED'
        ELSE total_score::text
    END as total_score,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'in_the_past_month_how_much_were_you_bothered_bybrfeeling')
        THEN 'REDACTED'
        ELSE in_the_past_month_how_much_were_you_bothered_bybrfeeling::text
    END as in_the_past_month_how_much_were_you_bothered_bybrfeeling,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'total_score_meaning')
        THEN 'REDACTED'
        ELSE total_score_meaning::text
    END as total_score_meaning,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'in_the_past_month_how_much_were_you_bothered_by_brrepeat')
        THEN 'REDACTED'
        ELSE in_the_past_month_how_much_were_you_bothered_by_brrepeat::text
    END as in_the_past_month_how_much_were_you_bothered_by_brrepeat,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'total_score_5')
        THEN 'REDACTED'
        ELSE total_score_5::text
    END as total_score_5,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'in_the_past_month_how_much_were_you_bothered_bybrloss_of')
        THEN 'REDACTED'
        ELSE in_the_past_month_how_much_were_you_bothered_bybrloss_of::text
    END as in_the_past_month_how_much_were_you_bothered_bybrloss_of,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'in_the_past_month_how_much_were_you_bothered_by_brhaving')
        THEN 'REDACTED'
        ELSE in_the_past_month_how_much_were_you_bothered_by_brhaving::text
    END as in_the_past_month_how_much_were_you_bothered_by_brhaving,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'in_the_past_month_how_much_were_you_bothered_by_bravoidi')
        THEN 'REDACTED'
        ELSE in_the_past_month_how_much_were_you_bothered_by_bravoidi::text
    END as in_the_past_month_how_much_were_you_bothered_by_bravoidi,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'total_score_meaning_9')
        THEN 'REDACTED'
        ELSE total_score_meaning_9::text
    END as total_score_meaning_9,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'how_difficult_have_these_problems_made_it_for_you_to_do_your')
        THEN 'REDACTED'
        ELSE how_difficult_have_these_problems_made_it_for_you_to_do_your::text
    END as how_difficult_have_these_problems_made_it_for_you_to_do_your,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'total_score_11')
        THEN 'REDACTED'
        ELSE total_score_11::text
    END as total_score_11,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'in_the_past_month_how_much_were_you_bothered_by_brfeelin')
        THEN 'REDACTED'
        ELSE in_the_past_month_how_much_were_you_bothered_by_brfeelin::text
    END as in_the_past_month_how_much_were_you_bothered_by_brfeelin,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'total_score_meaning_13')
        THEN 'REDACTED'
        ELSE total_score_meaning_13::text
    END as total_score_meaning_13,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'in_the_past_month_how_much_were_you_bothered_by_bravoidi_14')
        THEN 'REDACTED'
        ELSE in_the_past_month_how_much_were_you_bothered_by_bravoidi_14::text
    END as in_the_past_month_how_much_were_you_bothered_by_bravoidi_14,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'in_the_past_month_how_much_were_you_bothered_bybrhaving')
        THEN 'REDACTED'
        ELSE in_the_past_month_how_much_were_you_bothered_bybrhaving::text
    END as in_the_past_month_how_much_were_you_bothered_bybrhaving,
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'f1.10.1')
        THEN 'REDACTED'
        ELSE "f1.10.1"::text
    END as "f1.10.1",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'f1.11.1')
        THEN 'REDACTED'
        ELSE "f1.11.1"::text
    END as "f1.11.1",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'f1.15.1')
        THEN 'REDACTED'
        ELSE "f1.15.1"::text
    END as "f1.15.1",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'f1.16.1')
        THEN 'REDACTED'
        ELSE "f1.16.1"::text
    END as "f1.16.1",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'f1.17.1')
        THEN 'REDACTED'
        ELSE "f1.17.1"::text
    END as "f1.17.1",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'f1.18.1')
        THEN 'REDACTED'
        ELSE "f1.18.1"::text
    END as "f1.18.1",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'f1.19.1')
        THEN 'REDACTED'
        ELSE "f1.19.1"::text
    END as "f1.19.1",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'f1.20.1')
        THEN 'REDACTED'
        ELSE "f1.20.1"::text
    END as "f1.20.1",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'f1.21.1')
        THEN 'REDACTED'
        ELSE "f1.21.1"::text
    END as "f1.21.1",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'f1.3.1')
        THEN 'REDACTED'
        ELSE "f1.3.1"::text
    END as "f1.3.1",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'f1.4.1')
        THEN 'REDACTED'
        ELSE "f1.4.1"::text
    END as "f1.4.1",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'f1.5.1')
        THEN 'REDACTED'
        ELSE "f1.5.1"::text
    END as "f1.5.1",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'f1.6.1')
        THEN 'REDACTED'
        ELSE "f1.6.1"::text
    END as "f1.6.1",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'f1.7.1')
        THEN 'REDACTED'
        ELSE "f1.7.1"::text
    END as "f1.7.1",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'f1.8.1')
        THEN 'REDACTED'
        ELSE "f1.8.1"::text
    END as "f1.8.1",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'f1.9.1')
        THEN 'REDACTED'
        ELSE "f1.9.1"::text
    END as "f1.9.1",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'patient-age')
        THEN 'REDACTED'
        ELSE "patient-age"::text
    END as "patient-age",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'patient-dob')
        THEN 'REDACTED'
        ELSE "patient-dob"::text
    END as "patient-dob",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'patient-name')
        THEN 'REDACTED'
        ELSE "patient-name"::text
    END as "patient-name",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'show-pcl-5-to-8-questions')
        THEN 'REDACTED'
        ELSE "show-pcl-5-to-8-questions"::text
    END as "show-pcl-5-to-8-questions",
        CASE 
        WHEN EXISTS (SELECT 1 FROM metadata_pii WHERE question_alias = 'task-id-common-mental-pdf')
        THEN 'REDACTED'
        ELSE "task-id-common-mental-pdf"::text
    END as "task-id-common-mental-pdf",
        CURRENT_TIMESTAMP as anonymized_at

from source_data
  );