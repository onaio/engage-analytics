{% macro resource_level_1_extraction(level_1_keys, nested_keys) -%}
  select
  {%- for level_1_key in level_1_keys %}
    resource::jsonb ->> '{{ level_1_key }}' as {{ level_1_key }}
    {%- if not loop.last or (nested_keys|length > 0) %},{% endif %}
  {%- endfor %}

  {%- for nested_key in nested_keys %}
    jsonb_array_elements(resource::jsonb -> '{{ nested_key }}')::jsonb as {{ nested_key }}
    {%- if not loop.last %},{% endif %}
  {%- endfor %}
{%- endmacro -%}
