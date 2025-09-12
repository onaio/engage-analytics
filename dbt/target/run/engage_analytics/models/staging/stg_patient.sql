
      
        
        
        delete from "airbyte"."engage_analytics_engage_analytics_stg"."stg_patient" as DBT_INTERNAL_DEST
        where (id) in (
            select distinct id
            from "stg_patient__dbt_tmp131418333328" as DBT_INTERNAL_SOURCE
        );

    

    insert into "airbyte"."engage_analytics_engage_analytics_stg"."stg_patient" ("id", "meta", "gender", "resourcetype", "birthdate", "managingorganization", "deceasedboolean", "active", "text", "identifier", "telecom_value", "generalpractitioner_reference", "name_given", "name_family", "period_start", "_airbyte_emitted_at")
    (
        select "id", "meta", "gender", "resourcetype", "birthdate", "managingorganization", "deceasedboolean", "active", "text", "identifier", "telecom_value", "generalpractitioner_reference", "name_given", "name_family", "period_start", "_airbyte_emitted_at"
        from "stg_patient__dbt_tmp131418333328"
    )
  