
      
        
        
        delete from "airbyte"."engage_analytics_engage_analytics_mart"."organizations" as DBT_INTERNAL_DEST
        where (id) in (
            select distinct id
            from "organizations__dbt_tmp102534041950" as DBT_INTERNAL_SOURCE
        );

    

    insert into "airbyte"."engage_analytics_engage_analytics_mart"."organizations" ("id", "name", "active", "resource_id", "type_coding", "_airbyte_emitted_at")
    (
        select "id", "name", "active", "resource_id", "type_coding", "_airbyte_emitted_at"
        from "organizations__dbt_tmp102534041950"
    )
  