{% macro ref_id_from_reference(ref_text) %}
    -- ref_text looks like 'Patient/123' -> return '123'
    (regexp_replace({{ ref_text }}, '^\w+/', ''))
{% endmacro %}

{% macro as_timestamptz(expr) %}
  (nullif({{ expr }}, '')::timestamptz)
{% endmacro %}

{% macro jsonb_text(obj, key) %}
    (({{ obj }}) ->> {{ key }})
{% endmacro %}
