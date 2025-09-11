-- macros/metrics.sql

{% macro metrics_catalog() %}
  {% set metrics_yaml %}
- id: provider_count
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: practitioners
  expression: "count(distinct id)"
  description: "Total number of unique providers"
  version: v1
  
- id: patient_count
  unit: count
  grain: day
  entity_keys: [practitioner_organization_id]
  source_model: patient
  expression: "count(distinct id)"
  description: "Total number of unique patients"
  version: v1
  {% endset %}
  
  {% set parsed = fromyaml(metrics_yaml) %}
  {{ return(parsed) }}
{% endmacro %}

{% macro metric_sql(m) %}
  {%- if m.expression is defined -%}
    ({{ m.expression }})
  {%- else -%}
    ({{ m.numerator }})::numeric / {{ m.denominator }}
  {%- endif -%}
{% endmacro %}

{% macro metric_keys_clause(m) %}
  {{ return(m.get('entity_keys', ['organization_id']) | join(', ')) }}
{% endmacro %}