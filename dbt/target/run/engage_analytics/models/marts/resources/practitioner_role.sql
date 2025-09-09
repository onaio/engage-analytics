
      
        
        
        delete from "airbyte"."engage_analytics_engage_analytics_mart"."practitioner_role" as DBT_INTERNAL_DEST
        where (id) in (
            select distinct id
            from "practitioner_role__dbt_tmp094448932345" as DBT_INTERNAL_SOURCE
        );

    

    insert into "airbyte"."engage_analytics_engage_analytics_mart"."practitioner_role" ("id", "active", "resourcetype", "coding_code", "coding_display", "organization_id", "practitioner_id", "meta_lastupdated", "_airbyte_emitted_at")
    (
        select "id", "active", "resourcetype", "coding_code", "coding_display", "organization_id", "practitioner_id", "meta_lastupdated", "_airbyte_emitted_at"
        from "practitioner_role__dbt_tmp094448932345"
    )
  