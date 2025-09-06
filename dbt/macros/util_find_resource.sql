{% macro find_resource_id(column) %}
  split_part({{ column }}::jsonb->>'reference', '/', 2)
{% endmacro %}

{% macro find_resource_name(column) %}
  split_part({{ column }}::jsonb->>'reference', '/', 1)
{% endmacro %}
