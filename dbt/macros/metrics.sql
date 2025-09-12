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

- id: client_encounters
  unit: count
  grain: day
  entity_keys: [practitioner_organization_id]
  source_model: encounters
  expression: "count(distinct subject_id)"
  description: "Total unique clients seen (one encounter per patient per day)"
  version: v1

- id: active_providers
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: active_providers
  expression: "count(distinct case when is_active_30d then practitioner_id end)"
  description: "Providers who submitted QRs in past 30 days"
  version: v1

- id: percent_active_providers
  unit: percent
  grain: day
  entity_keys: [organization_id]
  source_model: active_providers
  numerator: "count(distinct case when is_active_30d then practitioner_id end)"
  denominator: "nullif(count(distinct practitioner_id), 0)"
  description: "Percentage of providers active in past 30 days"
  version: v1

- id: clients_eligible_for_ipc
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: clients_eligible_for_ipc
  expression: "count(distinct subject_patient_id)"
  description: "Clients eligible for IPC (Integrated Primary Care)"
  version: v1

- id: clients_sbirt_mi_eligible
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: clients_sbirt_mi_eligible
  expression: "count(distinct subject_patient_id)"
  description: "Clients eligible for SBIRT/MI (alcohol or drug problems)"
  version: v1

- id: clients_eligible_spi
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: clients_eligible_spi
  expression: "count(distinct subject_patient_id)"
  description: "Clients eligible for SPI (Suicide Prevention Intervention)"
  version: v1

- id: clients_eligible_referral
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: clients_eligible_referral
  expression: "count(distinct subject_patient_id)"
  description: "Clients eligible for referral (severe mental health)"
  version: v1

- id: clients_eligible_fws
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: clients_eligible_fws
  expression: "count(distinct subject_patient_id)"
  description: "Clients eligible for FWS (Financial Wellness Services)"
  version: v1
  {% endset %}
  
  {% set parsed = fromyaml(metrics_yaml) %}
  {{ return(parsed) }}
{% endmacro %}

{% macro metric_sql(m) %}
  {%- if m.expression is defined -%}
    ({{ m.expression }})
  {%- else -%}
    (({{ m.numerator }})::numeric / {{ m.denominator }} * 100)
  {%- endif -%}
{% endmacro %}

{% macro metric_keys_clause(m) %}
  {{ return(m.get('entity_keys', ['organization_id']) | join(', ')) }}
{% endmacro %}