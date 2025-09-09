
      
        
        
        delete from "airbyte"."engage_analytics_engage_analytics_mart"."practitioners" as DBT_INTERNAL_DEST
        where (id) in (
            select distinct id
            from "practitioners__dbt_tmp102534243418" as DBT_INTERNAL_SOURCE
        );

    

    insert into "airbyte"."engage_analytics_engage_analytics_mart"."practitioners" ("id", "active", "organization_id", "location_id", "name_family", "name_given", "phone_number", "_airbyte_emitted_at", "role_id", "role")
    (
        select "id", "active", "organization_id", "location_id", "name_family", "name_given", "phone_number", "_airbyte_emitted_at", "role_id", "role"
        from "practitioners__dbt_tmp102534243418"
    )
  