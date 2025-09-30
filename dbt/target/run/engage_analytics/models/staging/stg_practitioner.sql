
      
        
        
        delete from "airbyte"."engage_analytics_engage_analytics_stg"."stg_practitioner" as DBT_INTERNAL_DEST
        where (id) in (
            select distinct id
            from "stg_practitioner__dbt_tmp172800627734" as DBT_INTERNAL_SOURCE
        );

    

    insert into "airbyte"."engage_analytics_engage_analytics_stg"."stg_practitioner" ("id", "meta", "active", "identifier", "resourcetype", "name", "telecom", "_airbyte_emitted_at")
    (
        select "id", "meta", "active", "identifier", "resourcetype", "name", "telecom", "_airbyte_emitted_at"
        from "stg_practitioner__dbt_tmp172800627734"
    )
  