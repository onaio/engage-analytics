{% macro build_qr_wide_readable(identifier_list, relation_name) %}

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

  {# 1) collect distinct linkIds and their readable names for these questionnaires #}
  {% set q %}
    with ids(ident) as ( {{ ids_values }} ),
    table_name as (
      select '{{ relation_name }}' as table_name
    )
    select distinct
      a.linkid,
      coalesce(m.column, a.linkid) as readable_name,
      null::integer as question_order
    from {{ ref('int_qr_answers_long') }} a
    join ids on a.questionnaire_id = ids.ident
    cross join table_name t
    left join {{ ref('questionnaire_metadata') }} m
      on m."table" = t.table_name
      and m.linkid = a.linkid
    order by a.linkid
  {% endset %}

  {% set res = run_query(q) %}
  {% if execute %}
    {% set linkid_data = [] %}
    {% for row in res %}
      {% do linkid_data.append({'linkid': row[0], 'readable_name': row[1]}) %}
    {% endfor %}
  {% else %}
    {% set linkid_data = [] %}
  {% endif %}

  {# If there are no linkIds, return a valid empty view #}
  {% if linkid_data | length == 0 %}
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
      b.qr_id
      {%- set seen_names = [] -%}
      {%- for item in linkid_data -%}
        {%- set col_name = item.readable_name -%}
        {%- if col_name in seen_names -%}
          {%- set col_name = col_name ~ "_" ~ loop.index -%}
        {%- endif -%}
        {%- do seen_names.append(col_name) -%}
        ,max(case when linkid = '{{ item.linkid }}' then answer_value_text end) as "{{ col_name[:63] }}"
      {%- endfor %}
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