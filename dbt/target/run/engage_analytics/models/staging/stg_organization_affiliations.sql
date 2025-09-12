
      
        
        
        delete from "airbyte"."engage_analytics_engage_analytics_stg"."stg_organization_affiliations" as DBT_INTERNAL_DEST
        where (id) in (
            select distinct id
            from "stg_organization_affiliations__dbt_tmp131418317505" as DBT_INTERNAL_SOURCE
        );

    

    insert into "airbyte"."engage_analytics_engage_analytics_stg"."stg_organization_affiliations" ("id", "meta", "active", "resourcetype", "organization", "identifier", "location", "_airbyte_emitted_at")
    (
        select "id", "meta", "active", "resourcetype", "organization", "identifier", "location", "_airbyte_emitted_at"
        from "stg_organization_affiliations__dbt_tmp131418317505"
    )
  