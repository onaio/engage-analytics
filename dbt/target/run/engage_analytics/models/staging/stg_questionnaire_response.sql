
      
        
        
        delete from "airbyte"."engage_analytics_engage_analytics_stg"."stg_questionnaire_response" as DBT_INTERNAL_DEST
        where (id) in (
            select distinct id
            from "stg_questionnaire_response__dbt_tmp170513105752" as DBT_INTERNAL_SOURCE
        );

    

    insert into "airbyte"."engage_analytics_engage_analytics_stg"."stg_questionnaire_response" ("id", "resource", "status", "questionnaire_id", "subject_patient_id", "encounter_id", "author_practitioner_id", "items", "meta_lastupdated", "meta", "_airbyte_emitted_at")
    (
        select "id", "resource", "status", "questionnaire_id", "subject_patient_id", "encounter_id", "author_practitioner_id", "items", "meta_lastupdated", "meta", "_airbyte_emitted_at"
        from "stg_questionnaire_response__dbt_tmp170513105752"
    )
  