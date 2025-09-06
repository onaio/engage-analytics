{% macro build_qr_wide_any(identifier_list, relation_name) %}

  {# If no identifiers were provided, emit a valid empty-selectable view #}
  {% if identifier_list | length == 0 %}
    select
      null::text as qr_id,
      null::text as questionnaire_id,
      null::text as subject_patient_id,
      null::text as encounter_id,
      null::text as author_practitioner_id,
      null::text as practitioner_location_id,
      null::text as practitioner_organization_id,
      null::text as practitioner_id,
      null::text as practitioner_careteam_id,
      null::text as application_version
    where false

  {% else %}

  {# Render a VALUES(...) table of properly single-quoted strings #}
  {% set ids_values -%}
    values
    {%- for ident in identifier_list -%}
      ({{ "'" ~ ident | replace("'", "''") ~ "'" }})
      {%- if not loop.last %}, {% endif -%}
    {%- endfor -%}
  {%- endset %}

  {# 1) collect distinct linkIds for these questionnaires #}
  {% set q %}
    with ids(ident) as ( {{ ids_values }} )
    select distinct a.linkid
    from {{ ref('int_qr_answers_long') }} a
    join ids on a.questionnaire_id = ids.ident
  {% endset %}

  {% set res = run_query(q) %}
  {% if execute %}
    {% set linkids = res.columns[0].values() | list %}
  {% else %}
    {% set linkids = [] %}
  {% endif %}

  {# If there are no linkIds, return a valid empty view, avoiding a broken SELECT list #}
  {% if linkids | length == 0 %}
    select
      null::text as qr_id,
      null::text as questionnaire_id,
      null::text as subject_patient_id,
      null::text as encounter_id,
      null::text as author_practitioner_id,
      null::text as practitioner_location_id,
      null::text as practitioner_organization_id,
      null::text as practitioner_id,
      null::text as practitioner_careteam_id,
      null::text as application_version
    where false

  {% else %}

  with ids(ident) as ( {{ ids_values }} ),
  base as (
    select
      a.qr_id,
      a.linkid,
      string_agg(distinct a.answer_value_text, ' | ' order by a.answer_value_text) as answer_value_text
    from {{ ref('int_qr_answers_long') }} a
    join ids on a.questionnaire_id = ids.ident
    group by 1,2
  ),
  header as (
    select h.*
    from {{ ref('int_qr_header') }} h
    join ids on h.questionnaire_id = ids.ident
  ),
  tags as (
    select * from {{ ref('int_qr_tags') }}
  ),
  pivoted as (
    select
      b.qr_id,
      {{ dbt_utils.pivot(
          column='linkid',
          values=linkids,
          then_value='answer_value_text',
          agg='max',
          else_value='null'
      ) }}
    from base b
    group by b.qr_id
  )
  select
    h.questionnaire_id,
    h.subject_patient_id,
    h.encounter_id,
    h.author_practitioner_id,
    t.practitioner_location_id,
    t.practitioner_organization_id,
    t.practitioner_id,
    t.practitioner_careteam_id,
    t.application_version,
    p.*
  from pivoted p
  join header h using (qr_id)
  left join tags t on t.resource_id = h.qr_id

  {% endif %}
  {% endif %}
{% endmacro %}
