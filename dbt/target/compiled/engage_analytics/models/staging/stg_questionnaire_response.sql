

with src as (
  select
    id,
    resource,
    lower(resource::jsonb ->> 'status') as status,
    resource::jsonb ->> 'questionnaire' as questionnaire_id,
    split_part((resource::jsonb -> 'subject'    ->> 'reference'), '/', 2) as subject_patient_id,
    split_part((resource::jsonb -> 'encounter'  ->> 'reference'), '/', 2) as encounter_id,
    split_part((resource::jsonb -> 'author'     ->> 'reference'), '/', 2) as author_practitioner_id,
    resource::jsonb -> 'item' as items,
    resource::jsonb -> 'meta' ->> 'lastUpdated' as meta_lastupdated,
    resource::jsonb -> 'meta' as meta,
    _airbyte_extracted_at as _airbyte_emitted_at
  from "airbyte"."airbyte"."questionnaire_response"
  where lower(resource::jsonb ->> 'status') = 'completed'
)
select * from src

  where _airbyte_emitted_at > (select coalesce(max(_airbyte_emitted_at),'1900-01-01') from "airbyte"."engage_analytics_engage_analytics_stg"."stg_questionnaire_response")
