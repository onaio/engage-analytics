-- models/metrics/fct_metrics_long_incremental.sql
{{
    config(
        materialized='incremental',
        unique_key=['period_date', 'organization_id', 'metric_id'],
        on_schema_change='fail',
        incremental_strategy='merge'
    )
}}

{%- set metrics = metrics_catalog() -%}
{%- set lookback_days = 7 -%}  -- Reprocess last 7 days for late-arriving data

{# Collect distinct sources #}
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
    {%- else %}
    current_date as period_date,
    {%- endif %}
    {%- set all_keys = [] -%}
    {%- for m in metrics_list -%}
      {%- for key in m.get('entity_keys', ['organization_id']) -%}
        {%- if key not in all_keys -%}
          {%- do all_keys.append(key) -%}
        {%- endif -%}
      {%- endfor -%}
    {%- endfor -%}
    {{ all_keys | join(',\n    ') }},
    {%- if metrics_list[0].get('expression') %}
    {{ metrics_list[0].expression }} as {{ metrics_list[0].id }}_value
    {%- elif metrics_list[0].get('numerator') and metrics_list[0].get('denominator') %}
    ({{ metrics_list[0].numerator }}::decimal / {{ metrics_list[0].denominator }}) * 100 as {{ metrics_list[0].id }}_value
    {%- endif %}
    {%- for m in metrics_list[1:] %},
    {%- if m.get('expression') %}
    {{ m.expression }} as {{ m.id }}_value
    {%- elif m.get('numerator') and m.get('denominator') %}
    ({{ m.numerator }}::decimal / {{ m.denominator }}) * 100 as {{ m.id }}_value
    {%- endif %}
    {%- endfor %}
  from {{ ref(src) }}
  where 1=1
    {% if src == 'practitioners' or src == 'patient' %}
    and active = 'true'
    {% elif src == 'encounters' %}
    and period_start is not null
    {% endif %}
    
    {% if is_incremental() %}
      {%- if src == 'encounters' %}
      -- For encounters, reprocess recent days for late-arriving data
      and period_start::date >= current_date - interval '{{ lookback_days }} days'
      {%- else %}
      -- For dimension tables, always use current snapshot
      and 1=1
      {%- endif %}
    {% endif %}
    
  group by
    {%- if src == 'encounters' %}
    period_start::date,
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
    {{ m.id }}_value::numeric as value,
    '{{ m.unit }}' as unit,
    '{{ m.version }}' as method_version,
    '{{ m.description }}' as description,
    'prod' as status,
    current_timestamp as _last_updated
  from base_{{ src }}
  
  {% if is_incremental() %}
    -- Only include dates we're updating
    where 1=1
    {%- if src == 'encounters' %}
    and period_date >= current_date - interval '{{ lookback_days }} days'
    {%- else %}
    and period_date = current_date
    {%- endif %}
  {% endif %}
  
  {% if not loop.last %}union all{% endif %}
{%- endfor %}