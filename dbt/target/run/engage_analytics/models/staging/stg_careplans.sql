
      
        
        
        delete from "airbyte"."engage_analytics_engage_analytics_stg"."stg_careplans" as DBT_INTERNAL_DEST
        where (id) in (
            select distinct id
            from "stg_careplans__dbt_tmp234947770517" as DBT_INTERNAL_SOURCE
        );

    

    insert into "airbyte"."engage_analytics_engage_analytics_stg"."stg_careplans" ("id", "meta", "subject", "title", "author", "description", "resourcetype", "intent", "created", "period", "identifier", "status", "activity", "instantiatescanonical", "_airbyte_emitted_at")
    (
        select "id", "meta", "subject", "title", "author", "description", "resourcetype", "intent", "created", "period", "identifier", "status", "activity", "instantiatescanonical", "_airbyte_emitted_at"
        from "stg_careplans__dbt_tmp234947770517"
    )
  