
      
        
        
        delete from "airbyte"."engage_analytics_engage_analytics_stg"."stg_practitioner_roles" as DBT_INTERNAL_DEST
        where (id) in (
            select distinct id
            from "stg_practitioner_roles__dbt_tmp125935112454" as DBT_INTERNAL_SOURCE
        );

    

    insert into "airbyte"."engage_analytics_engage_analytics_stg"."stg_practitioner_roles" ("id", "meta", "active", "resourcetype", "organization", "practitioner", "code", "_airbyte_emitted_at")
    (
        select "id", "meta", "active", "resourcetype", "organization", "practitioner", "code", "_airbyte_emitted_at"
        from "stg_practitioner_roles__dbt_tmp125935112454"
    )
  