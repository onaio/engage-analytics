
      
        
        
        delete from "airbyte"."engage_analytics_engage_analytics_stg"."stg_locations" as DBT_INTERNAL_DEST
        where (id) in (
            select distinct id
            from "stg_locations__dbt_tmp120820907775" as DBT_INTERNAL_SOURCE
        );

    

    insert into "airbyte"."engage_analytics_engage_analytics_stg"."stg_locations" ("id", "meta", "description", "partof", "resourcetype", "status", "physicaltype", "name", "alias", "identifier", "_airbyte_emitted_at", "parent_name", "parent_id", "parent_reference")
    (
        select "id", "meta", "description", "partof", "resourcetype", "status", "physicaltype", "name", "alias", "identifier", "_airbyte_emitted_at", "parent_name", "parent_id", "parent_reference"
        from "stg_locations__dbt_tmp120820907775"
    )
  