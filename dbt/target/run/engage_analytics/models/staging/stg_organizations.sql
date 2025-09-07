
      
        
        
        delete from "airbyte"."engage_analytics_engage_analytics_stg"."stg_organizations" as DBT_INTERNAL_DEST
        where (id) in (
            select distinct id
            from "stg_organizations__dbt_tmp004854923193" as DBT_INTERNAL_SOURCE
        );

    

    insert into "airbyte"."engage_analytics_engage_analytics_stg"."stg_organizations" ("id", "meta", "name", "active", "identifier", "resourcetype", "type", "_airbyte_emitted_at")
    (
        select "id", "meta", "name", "active", "identifier", "resourcetype", "type", "_airbyte_emitted_at"
        from "stg_organizations__dbt_tmp004854923193"
    )
  