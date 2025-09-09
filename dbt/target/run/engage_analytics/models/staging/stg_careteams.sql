
      
        
        
        delete from "airbyte"."engage_analytics_engage_analytics_stg"."stg_careteams" as DBT_INTERNAL_DEST
        where (id) in (
            select distinct id
            from "stg_careteams__dbt_tmp102533620794" as DBT_INTERNAL_SOURCE
        );

    

    insert into "airbyte"."engage_analytics_engage_analytics_stg"."stg_careteams" ("id", "meta", "name", "status", "identifier", "resourcetype", "managingorganization", "participant", "_airbyte_emitted_at", "resource")
    (
        select "id", "meta", "name", "status", "identifier", "resourcetype", "managingorganization", "participant", "_airbyte_emitted_at", "resource"
        from "stg_careteams__dbt_tmp102533620794"
    )
  