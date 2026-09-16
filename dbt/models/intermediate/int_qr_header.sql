{{ config(materialized='view') }}

select
  id as qr_id,
  questionnaire_id,
  subject_patient_id,
  encounter_id,
  author_practitioner_id,
  -- Submission time, read from the stored resource so rows already in the incremental staging table are covered
  case when (resource::jsonb ->> 'authored') ~ '^\d{4}-\d{2}-\d{2}T'
    then (resource::jsonb ->> 'authored')::timestamptz end as authored_at,
  -- Submission date as recorded on the device (authored keeps its local offset, e.g. 2026-07-20T22:00:45+03:00)
  case when (resource::jsonb ->> 'authored') ~ '^\d{4}-\d{2}-\d{2}'
    then left(resource::jsonb ->> 'authored', 10)::date end as authored_date,
  _airbyte_emitted_at
from {{ ref('stg_questionnaire_response') }}
