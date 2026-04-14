-- depends_on: {{ ref('int_qr_tags') }}
-- depends_on: {{ ref('int_qr_header') }}
-- depends_on: {{ ref('questionnaire_metadata') }}
{{ config(materialized='view') }}
-- Short-Form PCL-5-8 (IPC Session 1) with readable column names
-- Questionnaire ID: pc-ptsd-ipc-session-1-questionnaire
-- Source file: questionnaire/ipc-session-1/sf-pcl-5-ipc-session-1.json
{% set identifiers = ["Questionnaire/pc-ptsd-ipc-session-1-questionnaire"] %}
{% if identifiers|length == 0 %}
-- No identifiers discovered for this Questionnaire; creating empty selectable view
select null::text as qr_id, null::text as questionnaire_id, null::text as subject_patient_id,
       null::text as encounter_id, null::text as author_practitioner_id
where false
{% else %}
{{ build_qr_wide_readable(identifiers, this.name) }}
{% endif %}