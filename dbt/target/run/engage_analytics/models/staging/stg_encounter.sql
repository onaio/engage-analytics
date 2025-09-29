
      
        
        
        delete from "airbyte"."engage_analytics_engage_analytics_stg"."stg_encounter" as DBT_INTERNAL_DEST
        where (id) in (
            select distinct id
            from "stg_encounter__dbt_tmp170512884398" as DBT_INTERNAL_SOURCE
        );

    

    insert into "airbyte"."engage_analytics_engage_analytics_stg"."stg_encounter" ("id", "meta", "subject", "servicetype", "resourcetype", "priority", "class", "serviceprovider", "reasoncode", "participant", "type", "period", "status", "_airbyte_emitted_at")
    (
        select "id", "meta", "subject", "servicetype", "resourcetype", "priority", "class", "serviceprovider", "reasoncode", "participant", "type", "period", "status", "_airbyte_emitted_at"
        from "stg_encounter__dbt_tmp170512884398"
    )
  