select
  pr.fhir_id as practitioner_id,
  coalesce(pr.given_name || ' ', '') || coalesce(pr.family_name,'') as practitioner_name,
  pr.phone_raw
from {{ ref('stg_practitioner') }} pr
