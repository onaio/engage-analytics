-- Macro to create anonymized views for any questionnaire response table
{% macro create_anonymized_qr_view(source_model, questionnaire_ids) %}

with metadata_pii as (
    select distinct
        question_alias,
        question_text,
        anon
    from {{ ref('questionnaire_metadata') }}
    where questionnaire_id in ({{ questionnaire_ids | join(', ') }})
),

source_data as (
    select * from {{ ref(source_model) }}
),

-- Get list of columns to check
column_info as (
    select 
        column_name,
        ordinal_position
    from information_schema.columns
    where table_schema = 'engage_analytics_engage_analytics_mart'
    and table_name = '{{ source_model }}'
    order by ordinal_position
)

select 
    {% set special_columns = ['qr_id', 'questionnaire_id', 'subject_patient_id', 'encounter_id', 
                             'author_practitioner_id', 'practitioner_location_id', 
                             'practitioner_organization_id', 'practitioner_id', 
                             'practitioner_careteam_id', 'application_version'] %}
    
    -- Hash patient and QR IDs for privacy
    MD5(qr_id::text) as qr_id_hash,
    questionnaire_id,
    MD5(COALESCE(subject_patient_id, '')::text) as subject_patient_id_hash,
    encounter_id,
    author_practitioner_id,
    practitioner_location_id,
    practitioner_organization_id,
    practitioner_id,
    practitioner_careteam_id,
    application_version,
    
    -- Process all other columns dynamically
    {% for col in get_columns_in_relation(ref(source_model)) %}
        {% if col.name not in special_columns %}
            -- Check if this column should be masked based on metadata
            CASE 
                WHEN EXISTS (
                    SELECT 1 FROM metadata_pii 
                    WHERE question_alias = '{{ col.name }}' 
                    AND anon = 'TRUE'
                )
                THEN 
                    {% if 'phone' in col.name.lower() %}
                        CASE 
                            WHEN {{ col.name }} IS NOT NULL AND LENGTH({{ col.name }}::text) >= 4
                            THEN 'XXX-XXX-' || RIGHT({{ col.name }}::text, 4)
                            ELSE 'NO PHONE'
                        END
                    {% elif 'email' in col.name.lower() %}
                        'REDACTED'
                    {% elif 'address' in col.name.lower() %}
                        'REDACTED'
                    {% elif 'zip' in col.name.lower() %}
                        CASE 
                            WHEN {{ col.name }} IS NOT NULL 
                            THEN LEFT({{ col.name }}::text, 3) || 'XX'
                            ELSE NULL
                        END
                    {% elif 'medicaid' in col.name.lower() and 'number' in col.name.lower() %}
                        'REDACTED'
                    {% elif 'ssn' in col.name.lower() or 'social_security' in col.name.lower() %}
                        'XXX-XX-' || RIGHT({{ col.name }}::text, 4)
                    {% else %}
                        'REDACTED'
                    {% endif %}
                ELSE {{ col.name }}::text
            END as {{ col.name }}_masked,
        {% endif %}
    {% endfor %}
    
    -- Add metadata
    CURRENT_TIMESTAMP as anonymized_at

from source_data

{% endmacro %}