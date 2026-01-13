
  create view "airbyte"."engage_analytics_engage_analytics_int"."int_qr_answers_long__dbt_tmp"
    
    
  as (
    

with recursive base as (
  select
    id as qr_id,
    questionnaire_id,
    subject_patient_id,
    encounter_id,
    author_practitioner_id,
    items,
    _airbyte_emitted_at
  from "airbyte"."engage_analytics_engage_analytics_stg"."stg_questionnaire_response"
),

recursive_items as (
  select
    b.qr_id,
    b.questionnaire_id,
    b.subject_patient_id,
    b.encounter_id,
    b.author_practitioner_id,
    (i.value)::jsonb as item_json,
    array[(i.value)::jsonb ->> 'linkId']::text[] as linkid_path,
    b._airbyte_emitted_at
  from base b
  cross join lateral jsonb_array_elements(coalesce(b.items,'[]'::jsonb)) as i(value)
  union all
  select
    r.qr_id,
    r.questionnaire_id,
    r.subject_patient_id,
    r.encounter_id,
    r.author_practitioner_id,
    child.value::jsonb as item_json,
    (r.linkid_path || (child.value::jsonb ->> 'linkId'))::text[] as linkid_path,
    r._airbyte_emitted_at
  from recursive_items r
  join lateral jsonb_array_elements(coalesce(r.item_json -> 'item','[]'::jsonb)) as child(value) on true
),

answers as (
  select
    qr_id,
    questionnaire_id,
    subject_patient_id,
    encounter_id,
    author_practitioner_id,
    (linkid_path[array_length(linkid_path,1)]) as linkid,
    a.value::jsonb as ans,
    _airbyte_emitted_at
  from recursive_items
  cross join lateral jsonb_array_elements(coalesce(item_json -> 'answer','[]'::jsonb)) as a(value)
),

normalized as (
  select
    qr_id,
    questionnaire_id,
    subject_patient_id,
    encounter_id,
    author_practitioner_id,
    linkid,
    ans ->> 'valueString'                         as value_string,
    (ans ->> 'valueInteger')::int                 as value_integer,
    (ans ->> 'valueDecimal')::numeric             as value_decimal,
    ans ->> 'valueDate'                           as value_date,
    ans -> 'valueCoding' ->> 'code'               as value_coding_code,
    ans -> 'valueCoding' ->> 'display'            as value_coding_display,
    (ans -> 'valueQuantity' ->> 'value')::numeric as value_quantity_value,
    ans -> 'valueQuantity' ->> 'unit'             as value_quantity_unit,
    coalesce(
      ans -> 'valueCoding' ->> 'display',
      ans ->> 'valueString',
      ans ->> 'valueDate',
      ans ->> 'valueInteger',
      ans ->> 'valueDecimal',
      ans -> 'valueQuantity' ->> 'value'
    ) as answer_value_text,
    _airbyte_emitted_at
  from answers
)

select * from normalized
  );