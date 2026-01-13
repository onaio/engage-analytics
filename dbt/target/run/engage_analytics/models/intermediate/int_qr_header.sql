
  create view "airbyte"."engage_analytics_engage_analytics_int"."int_qr_header__dbt_tmp"
    
    
  as (
    

select
  id as qr_id,
  questionnaire_id,
  subject_patient_id,
  encounter_id,
  author_practitioner_id,
  _airbyte_emitted_at
from "airbyte"."engage_analytics_engage_analytics_stg"."stg_questionnaire_response"
  );