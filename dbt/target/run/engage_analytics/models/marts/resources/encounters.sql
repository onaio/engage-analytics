
      
        
        
        delete from "airbyte"."engage_analytics_engage_analytics_mart"."encounters" as DBT_INTERNAL_DEST
        where (id) in (
            select distinct id
            from "encounters__dbt_tmp172800913843" as DBT_INTERNAL_SOURCE
        );

    

    insert into "airbyte"."engage_analytics_engage_analytics_mart"."encounters" ("id", "status", "reasoncode", "subject_type", "subject_id", "month", "service_coding", "service_display", "serviceprovider_reference", "class_code", "priority_text", "period_start", "period_end", "location_id", "practitioner_organization_id", "practitioner_careteam_id", "practitioner_id", "participant", "_airbyte_emitted_at")
    (
        select "id", "status", "reasoncode", "subject_type", "subject_id", "month", "service_coding", "service_display", "serviceprovider_reference", "class_code", "priority_text", "period_start", "period_end", "location_id", "practitioner_organization_id", "practitioner_careteam_id", "practitioner_id", "participant", "_airbyte_emitted_at"
        from "encounters__dbt_tmp172800913843"
    )
  