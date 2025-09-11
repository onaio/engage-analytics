-- models/metrics/fct_metrics_long.sql
{{
    config(
        materialized='table'
    )
}}

{%- set metrics = metrics_catalog() -%}

{# Collect distinct sources to pre-aggregate once per source_model #}
{%- set by_source = {} -%}
{%- for m in metrics -%}
  {%- set src = m.get('source_model') -%}
  {%- if src not in by_source -%}
    {%- do by_source.update({src: []}) -%}
  {%- endif -%}
  {%- do by_source[src].append(m) -%}
{%- endfor -%}

with
{# Pre-aggregate data per source_model #}
{%- for src, metrics_list in by_source.items() %}
base_{{ src }} as (
  select
    current_date as period_date,
    {%- for m in metrics_list %}
    {%- for key in m.get('entity_keys', ['organization_id']) %}
    {{ key }},
    {%- endfor -%}
    {%- endfor %}
    {{ metrics_list[0].expression }} as {{ metrics_list[0].id }}_value
    {%- for m in metrics_list[1:] %},
    {{ m.expression }} as {{ m.id }}_value
    {%- endfor %}
  from {{ ref(src) }}
  {% if src == 'practitioners' or src == 'patient' %}
  where active = 'true'
  {% endif %}
  group by
    {%- set all_keys = [] -%}
    {%- for m in metrics_list -%}
      {%- for key in m.get('entity_keys', ['organization_id']) -%}
        {%- if key not in all_keys -%}
          {%- do all_keys.append(key) -%}
        {%- endif -%}
      {%- endfor -%}
    {%- endfor -%}
    {{ ' ' }}{{ all_keys | join(', ') }}
){% if not loop.last %},{% endif %}
{%- endfor -%}

{# Emit one SELECT per metric and UNION ALL #}
{%- for m in metrics %}
  {%- set src = m.get('source_model') %}
  select
    period_date,
    {{ metric_keys_clause(m) }},
    '{{ m.id }}' as metric_id,
    {{ m.id }}_value::numeric as value,
    '{{ m.unit }}' as unit,
    '{{ m.version }}' as method_version,
    '{{ m.description }}' as description,
    'prod' as status
  from base_{{ src }}
  {% if not loop.last %}union all{% endif %}
{%- endfor %}