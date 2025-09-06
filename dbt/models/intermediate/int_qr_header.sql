{{ config(materialized='view') }}

select
  id as qr_id,
  questionnaire_id,
  subject_patient_id,
  encounter_id,
  author_practitioner_id,
  _airbyte_emitted_at
from {{ ref('stg_questionnaire_response') }}
