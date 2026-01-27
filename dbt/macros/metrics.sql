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

- id: clients_accepted_ipc
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: clients_accepted_ipc
  expression: "count(distinct subject_patient_id)"
  description: "Clients who accepted IPC (scheduled IPC session)"
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

- id: encounters_in_person
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: encounters_by_delivery_format
  expression: "count(distinct case when format_you_deliver = 'In-person' then subject_patient_id end)"
  description: "Daily in-person encounters"
  version: v1

- id: encounters_video_telehealth
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: encounters_by_delivery_format
  expression: "count(distinct case when format_you_deliver = 'Video telehealth' then subject_patient_id end)"
  description: "Daily video telehealth encounters"
  version: v1

- id: encounters_phone_telehealth
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: encounters_by_delivery_format
  expression: "count(distinct case when format_you_deliver = 'Phone telehealth' then subject_patient_id end)"
  description: "Daily phone telehealth encounters"
  version: v1

- id: ipc_clients_started
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: client_ipc_progress
  expression: "count(distinct subject_patient_id)"
  description: "Clients who started IPC (completed session 1)"
  version: v1

- id: ipc_clients_completed
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: client_ipc_progress
  expression: "count(distinct case when completed_all_sessions then subject_patient_id end)"
  description: "Clients who completed all 4 IPC sessions"
  version: v1

- id: ipc_completion_rate
  unit: percent
  grain: day
  entity_keys: [organization_id]
  source_model: client_ipc_progress
  numerator: "count(distinct case when completed_all_sessions then subject_patient_id end)"
  denominator: "nullif(count(distinct subject_patient_id), 0)"
  description: "Percentage of IPC clients who completed all 4 sessions"
  version: v1

- id: ipc_retention_s1_to_s2
  unit: percent
  grain: day
  entity_keys: [organization_id]
  source_model: client_ipc_progress
  numerator: "count(distinct case when completed_s2 then subject_patient_id end)"
  denominator: "nullif(count(distinct subject_patient_id), 0)"
  description: "Percentage of IPC clients who returned for session 2"
  version: v1

- id: phq9_clients_severe
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: int_assessment_scores
  expression: "count(distinct case when assessment_type = 'PHQ-9' and severity = 'Severe' then subject_patient_id end)"
  description: "Clients with severe PHQ-9 scores (20-27)"
  version: v1

- id: phq9_clients_moderate_high
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: int_assessment_scores
  expression: "count(distinct case when assessment_type = 'PHQ-9' and severity in ('Moderate', 'Moderately Severe') then subject_patient_id end)"
  description: "Clients with moderate/moderately severe PHQ-9 scores (10-19)"
  version: v1

- id: phq9_clients_low
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: int_assessment_scores
  expression: "count(distinct case when assessment_type = 'PHQ-9' and severity in ('Mild', 'Minimal') then subject_patient_id end)"
  description: "Clients with mild/minimal PHQ-9 scores (0-9)"
  version: v1

- id: phq9_percent_severe
  unit: percent
  grain: day
  entity_keys: [organization_id]
  source_model: int_assessment_scores
  numerator: "count(distinct case when assessment_type = 'PHQ-9' and severity = 'Severe' then subject_patient_id end)"
  denominator: "nullif(count(distinct case when assessment_type = 'PHQ-9' then subject_patient_id end), 0)"
  description: "Percentage of PHQ-9 clients scoring severe"
  version: v1

- id: phq9_percent_moderate_high
  unit: percent
  grain: day
  entity_keys: [organization_id]
  source_model: int_assessment_scores
  numerator: "count(distinct case when assessment_type = 'PHQ-9' and severity in ('Moderate', 'Moderately Severe') then subject_patient_id end)"
  denominator: "nullif(count(distinct case when assessment_type = 'PHQ-9' then subject_patient_id end), 0)"
  description: "Percentage of PHQ-9 clients scoring moderate/moderately severe"
  version: v1

- id: gad7_clients_severe
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: int_assessment_scores
  expression: "count(distinct case when assessment_type = 'GAD-7' and severity = 'Severe' then subject_patient_id end)"
  description: "Clients with severe GAD-7 scores (15-21)"
  version: v1

- id: gad7_clients_moderate_high
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: int_assessment_scores
  expression: "count(distinct case when assessment_type = 'GAD-7' and severity = 'Moderate' then subject_patient_id end)"
  description: "Clients with moderate GAD-7 scores (10-14)"
  version: v1

- id: gad7_clients_low
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: int_assessment_scores
  expression: "count(distinct case when assessment_type = 'GAD-7' and severity in ('Mild', 'Minimal') then subject_patient_id end)"
  description: "Clients with mild/minimal GAD-7 scores (0-9)"
  version: v1

- id: gad7_percent_severe
  unit: percent
  grain: day
  entity_keys: [organization_id]
  source_model: int_assessment_scores
  numerator: "count(distinct case when assessment_type = 'GAD-7' and severity = 'Severe' then subject_patient_id end)"
  denominator: "nullif(count(distinct case when assessment_type = 'GAD-7' then subject_patient_id end), 0)"
  description: "Percentage of GAD-7 clients scoring severe"
  version: v1

- id: gad7_percent_moderate_high
  unit: percent
  grain: day
  entity_keys: [organization_id]
  source_model: int_assessment_scores
  numerator: "count(distinct case when assessment_type = 'GAD-7' and severity = 'Moderate' then subject_patient_id end)"
  denominator: "nullif(count(distinct case when assessment_type = 'GAD-7' then subject_patient_id end), 0)"
  description: "Percentage of GAD-7 clients scoring moderate"
  version: v1

- id: sessions_total
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: intervention_sessions
  expression: "count(distinct qr_id)"
  description: "Total counseling sessions across all intervention types"
  version: v1

- id: sessions_ipc
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: intervention_sessions
  expression: "count(distinct case when intervention_type = 'IPC' then qr_id end)"
  description: "IPC (Interpersonal Counseling) sessions"
  version: v1

- id: sessions_spi
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: intervention_sessions
  expression: "count(distinct case when intervention_type = 'SPI' then qr_id end)"
  description: "SPI (Suicide Prevention Intervention) sessions"
  version: v1

- id: sessions_sbirt
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: intervention_sessions
  expression: "count(distinct case when intervention_type = 'SBIRT' then qr_id end)"
  description: "SBIRT (Screening, Brief Intervention, Referral to Treatment) sessions"
  version: v1

- id: clients_receiving_counseling
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: intervention_sessions
  expression: "count(distinct subject_patient_id)"
  description: "Unique clients receiving any counseling session"
  version: v1

- id: mwtool_ipc_eligible_initiated
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: mwtool_to_intervention
  expression: "count(distinct case when intervention_type = 'IPC' and initiated_treatment then subject_patient_id end)"
  description: "Clients eligible for IPC who initiated treatment"
  version: v1

- id: mwtool_ipc_conversion_rate
  unit: percent
  grain: day
  entity_keys: [organization_id]
  source_model: mwtool_to_intervention
  numerator: "count(distinct case when intervention_type = 'IPC' and initiated_treatment then subject_patient_id end)"
  denominator: "nullif(count(distinct case when intervention_type = 'IPC' then subject_patient_id end), 0)"
  description: "Percentage of IPC-eligible clients who initiated treatment"
  version: v1

- id: mwtool_sbirt_eligible_initiated
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: mwtool_to_intervention
  expression: "count(distinct case when intervention_type = 'SBIRT' and initiated_treatment then subject_patient_id end)"
  description: "Clients eligible for SBIRT who initiated treatment"
  version: v1

- id: mwtool_sbirt_conversion_rate
  unit: percent
  grain: day
  entity_keys: [organization_id]
  source_model: mwtool_to_intervention
  numerator: "count(distinct case when intervention_type = 'SBIRT' and initiated_treatment then subject_patient_id end)"
  denominator: "nullif(count(distinct case when intervention_type = 'SBIRT' then subject_patient_id end), 0)"
  description: "Percentage of SBIRT-eligible clients who initiated treatment"
  version: v1

- id: mwtool_spi_eligible_initiated
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: mwtool_to_intervention
  expression: "count(distinct case when intervention_type = 'SPI' and initiated_treatment then subject_patient_id end)"
  description: "Clients eligible for SPI who initiated treatment"
  version: v1

- id: mwtool_spi_conversion_rate
  unit: percent
  grain: day
  entity_keys: [organization_id]
  source_model: mwtool_to_intervention
  numerator: "count(distinct case when intervention_type = 'SPI' and initiated_treatment then subject_patient_id end)"
  denominator: "nullif(count(distinct case when intervention_type = 'SPI' then subject_patient_id end), 0)"
  description: "Percentage of SPI-eligible clients who initiated treatment"
  version: v1

- id: phq9_clients_with_followup
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: assessment_scores_all_sessions
  expression: "count(distinct subject_patient_id)"
  description: "PHQ-9 clients with multiple session scores (can measure change)"
  version: v1

- id: phq9_clients_improved
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: assessment_scores_all_sessions
  expression: "count(distinct case when improved then subject_patient_id end)"
  description: "PHQ-9 clients whose score improved over treatment"
  version: v1

- id: phq9_improvement_rate
  unit: percent
  grain: day
  entity_keys: [organization_id]
  source_model: assessment_scores_all_sessions
  numerator: "count(distinct case when improved then subject_patient_id end)"
  denominator: "nullif(count(distinct subject_patient_id), 0)"
  description: "Percentage of PHQ-9 clients who showed score improvement"
  version: v1

- id: phq9_remission_rate
  unit: percent
  grain: day
  entity_keys: [organization_id]
  source_model: assessment_scores_all_sessions
  numerator: "count(distinct case when achieved_remission then subject_patient_id end)"
  denominator: "nullif(count(distinct subject_patient_id), 0)"
  description: "Percentage of PHQ-9 clients who achieved remission (score < 10)"
  version: v1

- id: phq9_avg_score_improvement
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: assessment_scores_all_sessions
  expression: "round(avg(score_improvement)::numeric, 1)"
  description: "Average PHQ-9 score improvement (first minus last session)"
  version: v1

- id: observation_count
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: stg_observation
  expression: "count(distinct observation_id)"
  description: "Total observations (computed assessment scores)"
  version: v1

- id: observation_phq9_count
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: stg_observation
  expression: "count(distinct case when observation_code like 'phq9%' then observation_id end)"
  description: "PHQ-9 score observations"
  version: v1

- id: observation_gad7_count
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: stg_observation
  expression: "count(distinct case when observation_code like 'gad7%' then observation_id end)"
  description: "GAD-7 score observations"
  version: v1

- id: observation_mood_count
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: stg_observation
  expression: "count(distinct case when observation_code like 'mood%' then observation_id end)"
  description: "Mood rating observations"
  version: v1

- id: observation_ptsd_count
  unit: count
  grain: day
  entity_keys: [organization_id]
  source_model: stg_observation
  expression: "count(distinct case when observation_code like 'ptsd%' then observation_id end)"
  description: "PTSD (PCL-5) score observations"
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