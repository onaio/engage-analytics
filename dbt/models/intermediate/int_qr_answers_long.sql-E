with qr as (
  select * from {{ ref('stg_questionnaire_response') }}
),
items as (
  select
    q.fhir_id as qr_id,
    q.patient_id,
    q.encounter_id,
    q.author_id,
    q.questionnaire_canonical,
    q.authored_ts,
    i ->> 'linkId' as link_id,
    i ->> 'text'   as item_text,
    a as answer
  from qr q
  left join lateral jsonb_array_elements(q.resource -> 'item') as i on true
  left join lateral jsonb_array_elements(coalesce(i->'answer','[]'::jsonb)) as a on true
),
answers as (
  select
    *,
    case
      when answer ? 'valueInteger' then (answer ->> 'valueInteger')::numeric
      when answer ? 'valueDecimal' then (answer ->> 'valueDecimal')::numeric
      when answer ? 'valueQuantity' then (answer -> 'valueQuantity' ->> 'value')::numeric
      else null
    end as value_num,
    answer -> 'valueCoding' ->> 'code'    as value_code,
    answer -> 'valueCoding' ->> 'display' as value_display,
    answer ->> 'valueString'              as value_string,
    answer ->> 'valueDate'                as value_date,
    answer ->> 'valueBoolean'             as value_boolean
  from items
)
select * from answers;
