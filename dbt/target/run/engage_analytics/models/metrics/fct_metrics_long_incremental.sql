
      -- back compat for old kwarg name
  
  
        
            
                
                
            
                
                
            
                
                
            
        
    

    

    merge into "airbyte"."engage_analytics"."fct_metrics_long_incremental" as DBT_INTERNAL_DEST
        using "fct_metrics_long_incremental__dbt_tmp170521782736" as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.period_date = DBT_INTERNAL_DEST.period_date
                ) and (
                    DBT_INTERNAL_SOURCE.organization_id = DBT_INTERNAL_DEST.organization_id
                ) and (
                    DBT_INTERNAL_SOURCE.metric_id = DBT_INTERNAL_DEST.metric_id
                )

    
    when matched then update set
        "period_date" = DBT_INTERNAL_SOURCE."period_date","organization_id" = DBT_INTERNAL_SOURCE."organization_id","metric_id" = DBT_INTERNAL_SOURCE."metric_id","value" = DBT_INTERNAL_SOURCE."value","unit" = DBT_INTERNAL_SOURCE."unit","method_version" = DBT_INTERNAL_SOURCE."method_version","description" = DBT_INTERNAL_SOURCE."description","status" = DBT_INTERNAL_SOURCE."status","_last_updated" = DBT_INTERNAL_SOURCE."_last_updated"
    

    when not matched then insert
        ("period_date", "organization_id", "metric_id", "value", "unit", "method_version", "description", "status", "_last_updated")
    values
        ("period_date", "organization_id", "metric_id", "value", "unit", "method_version", "description", "status", "_last_updated")


  