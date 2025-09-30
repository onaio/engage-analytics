
      
        
        
        delete from "airbyte"."engage_analytics_engage_analytics_stg"."stg_task" as DBT_INTERNAL_DEST
        where (id) in (
            select distinct id
            from "stg_task__dbt_tmp141916276044" as DBT_INTERNAL_SOURCE
        );

    

    insert into "airbyte"."engage_analytics_engage_analytics_stg"."stg_task" ("id", "meta", "authoredon", "basedon", "statusreason", "reasonreference", "executionperiod", "for", "code", "owner", "description", "resourcetype", "intent", "priority", "lastmodified", "requester", "status", "reasoncode", "_airbyte_emitted_at")
    (
        select "id", "meta", "authoredon", "basedon", "statusreason", "reasonreference", "executionperiod", "for", "code", "owner", "description", "resourcetype", "intent", "priority", "lastmodified", "requester", "status", "reasoncode", "_airbyte_emitted_at"
        from "stg_task__dbt_tmp141916276044"
    )
  