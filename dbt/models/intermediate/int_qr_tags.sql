{{ config(materialized='incremental', unique_key='resource_id') }}
{{ meta_tag_pivot('stg_questionnaire_response') }}
