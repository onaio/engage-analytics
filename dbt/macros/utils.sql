{% macro ref_id_from_reference(ref_text) %}
    -- ref_text looks like 'Patient/123' -> return '123'
    (regexp_replace({{ ref_text }}, '^\w+/', ''))
{% endmacro %}

{% macro as_timestamptz(text_col) %}
    -- Handle ISO strings from FHIR meta/fields safely
    ({{ text_col }}::timestamptz)
{% endmacro %}

{% macro jsonb_text(obj, key) %}
    (({{ obj }}) ->> {{ key }})
{% endmacro %}
