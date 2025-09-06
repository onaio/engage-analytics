{#---- SAFE jsonb Level-2 extractor for OBJECT columns only ----#}
{% macro resource_level_2_extraction(resource_name, json_columns) -%}
  select
    id::text as resource_id,
    {%- for col in json_columns %}
      {%- set is_last_col = loop.last %}
      {%- set query -%}
        /* only inspect rows where the column is a JSON OBJECT */
        select distinct jsonb_object_keys({{ col }}::jsonb)
        from {{ ref(resource_name) }}
        where jsonb_typeof({{ col }}::jsonb) = 'object'
      {%- endset -%}

      {%- set results = run_query(query) -%}
      {%- if execute -%}
        {%- set results_list = results.columns[0].values() | list -%}
      {%- else -%}
        {%- set results_list = [] -%}
      {%- endif -%}

      {%- for key in results_list %}
        {{ col }}::jsonb ->> '{{ key }}' as {{ col }}_{{ key }}{% if not (loop.last and is_last_col) %},{% endif %}
      {%- endfor %}
    {%- endfor %},
    /* use the column your staging models actually expose */
    _airbyte_emitted_at
  from {{ ref(resource_name) }}
{%- endmacro -%}

