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
    {%- if src == 'encounters' %}
    period_start::date as period_date,
    {%- elif src == 'clients_with_mental_health' %}
    period_date,
    {%- else %}
    current_date as period_date,
    {%- endif %}
    {%- set all_entity_keys = [] -%}
    {%- for m in metrics_list -%}
      {%- for key in m.get('entity_keys', ['organization_id']) -%}
        {%- if key not in all_entity_keys -%}
          {%- do all_entity_keys.append(key) -%}
        {%- endif -%}
      {%- endfor -%}
    {%- endfor -%}
    {%- for key in all_entity_keys %}
    {{ key }},
    {%- endfor %}
    {%- for m in metrics_list %}
    {%- if m.expression is defined %}
    {{ m.expression }} as {{ m.id }}_value
    {%- else %}
    {{ m.numerator }} as {{ m.id }}_numerator,
    {{ m.denominator }} as {{ m.id }}_denominator
    {%- endif %}
    {%- if not loop.last %},{% endif %}
    {%- endfor %}
  from {{ ref(src) }}
  {% if src == 'practitioners' or src == 'patient' %}
  where active = 'true'
  {% elif src == 'encounters' %}
  where period_start is not null
  {% endif %}
  group by
    {%- if src == 'encounters' %}
    period_start::date,
    {%- elif src == 'clients_with_mental_health' %}
    period_date,
    {%- endif %}
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
    {%- if m.expression is defined %}
    {{ m.id }}_value::numeric as value,
    {%- else %}
    ({{ m.id }}_numerator::numeric / {{ m.id }}_denominator * 100) as value,
    {%- endif %}
    '{{ m.unit }}' as unit,
    '{{ m.version }}' as method_version,
    '{{ m.description }}' as description,
    'prod' as status
  from base_{{ src }}
  {% if not loop.last %}union all{% endif %}
{%- endfor %}