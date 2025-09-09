
      
        
        
        delete from "airbyte"."engage_analytics_engage_analytics_mart"."patient" as DBT_INTERNAL_DEST
        where (id) in (
            select distinct id
            from "patient__dbt_tmp102534018111" as DBT_INTERNAL_SOURCE
        );

    

    insert into "airbyte"."engage_analytics_engage_analytics_mart"."patient" ("id", "gender", "birth_date", "age_years", "deceased", "active", "registration_date", "location_id", "name_family", "name_given", "phone_number", "practitioner_id", "practitioner_organization_id", "practitioner_careteam_id", "_airbyte_emitted_at")
    (
        select "id", "gender", "birth_date", "age_years", "deceased", "active", "registration_date", "location_id", "name_family", "name_given", "phone_number", "practitioner_id", "practitioner_organization_id", "practitioner_careteam_id", "_airbyte_emitted_at"
        from "patient__dbt_tmp102534018111"
    )
  