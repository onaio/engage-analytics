{% config(materialized='view') %}
-- Mood Rating (IPC Session 1) (id: ipc-203, url: n/a, version: 0.0.1)
{% set identifiers = ["Questionnaire/ipc-203"] %}
{% if identifiers|length == 0 %}
-- No identifiers discovered for this Questionnaire; creating empty selectable view
select null::text as qr_id, null::text as questionnaire_id, null::text as subject_patient_id,
       null::text as encounter_id, null::text as author_practitioner_id
where false
{% else %}
{ build_qr_wide_any(identifiers, this.name) }
{% endif %}
