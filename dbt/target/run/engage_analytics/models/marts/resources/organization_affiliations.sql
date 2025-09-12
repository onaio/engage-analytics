
      
        
        
        delete from "airbyte"."engage_analytics_engage_analytics_mart"."organization_affiliations" as DBT_INTERNAL_DEST
        where (id) in (
            select distinct id
            from "organization_affiliations__dbt_tmp131418494566" as DBT_INTERNAL_SOURCE
        );

    

    insert into "airbyte"."engage_analytics_engage_analytics_mart"."organization_affiliations" ("id", "organization_id", "location_id", "_airbyte_emitted_at")
    (
        select "id", "organization_id", "location_id", "_airbyte_emitted_at"
        from "organization_affiliations__dbt_tmp131418494566"
    )
  