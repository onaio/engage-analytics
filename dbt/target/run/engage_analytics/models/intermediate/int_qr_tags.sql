
      
        
        
        delete from "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags" as DBT_INTERNAL_DEST
        where (resource_id) in (
            select distinct resource_id
            from "int_qr_tags__dbt_tmp140304325644" as DBT_INTERNAL_SOURCE
        );

    

    insert into "airbyte"."engage_analytics_engage_analytics_int"."int_qr_tags" ("resource_id", "_airbyte_emitted_at", "practitioner_location_id", "practitioner_organization_id", "practitioner_id", "practitioner_careteam_id", "application_version")
    (
        select "resource_id", "_airbyte_emitted_at", "practitioner_location_id", "practitioner_organization_id", "practitioner_id", "practitioner_careteam_id", "application_version"
        from "int_qr_tags__dbt_tmp140304325644"
    )
  