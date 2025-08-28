select
  e.fhir_id as encounter_id,
  e.patient_id,
  e.practitioner_id,
  e.status,
  e.class_code,
  e.type_code,
  e.type_display,
  e.period_start,
  e.period_end
from {{ ref('stg_encounter') }} e;
