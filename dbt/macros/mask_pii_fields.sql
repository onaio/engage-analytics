-- Macro to dynamically mask PII fields based on questionnaire_metadata.anon flag
{% macro mask_pii_fields(questionnaire_id) %}
    {% set query %}
        SELECT 
            question_alias,
            question_text
        FROM {{ ref('questionnaire_metadata') }}
        WHERE questionnaire_id = '{{ questionnaire_id }}'
        AND anon = 'TRUE'
    {% endset %}
    
    {% set results = run_query(query) %}
    
    {% if execute %}
        {% set pii_fields = [] %}
        {% for row in results %}
            {% do pii_fields.append(row[0]) %}
        {% endfor %}
        {{ return(pii_fields) }}
    {% else %}
        {{ return([]) }}
    {% endif %}
{% endmacro %}

-- Macro to generate masked select statements for PII fields
{% macro generate_masked_select(questionnaire_id, table_alias='') %}
    {% set pii_fields = mask_pii_fields(questionnaire_id) %}
    {% set table_prefix = table_alias ~ '.' if table_alias else '' %}
    
    {% set masked_columns = [] %}
    
    -- Get all columns from the table
    {% set all_columns_query %}
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'engage_analytics_engage_analytics_mart' 
        AND table_name = 'qr_registration_info'
        ORDER BY ordinal_position
    {% endset %}
    
    {% if execute %}
        {% set all_columns_result = run_query(all_columns_query) %}
        {% set all_columns = [] %}
        {% for row in all_columns_result %}
            {% do all_columns.append(row[0]) %}
        {% endfor %}
        
        -- Generate select statement with masking
        {% for col in all_columns %}
            {% if col in pii_fields %}
                -- Special handling for different field types
                {% if 'phone' in col.lower() %}
                    {% do masked_columns.append("CASE WHEN " ~ table_prefix ~ '"' ~ col ~ '" IS NOT NULL AND LENGTH(' ~ table_prefix ~ '"' ~ col ~ '") >= 4 THEN \'XXX-XXX-\' || RIGHT(' ~ table_prefix ~ '"' ~ col ~ '", 4) ELSE \'NO PHONE\' END as "' ~ col ~ '_masked"') %}
                {% elif 'email' in col.lower() %}
                    {% do masked_columns.append("'REDACTED' as \"" ~ col ~ "_masked\"") %}
                {% elif 'address' in col.lower() and 'physical' in col.lower() %}
                    {% do masked_columns.append("'REDACTED' as \"" ~ col ~ "_masked\"") %}
                {% elif 'zip' in col.lower() %}
                    {% do masked_columns.append("CASE WHEN " ~ table_prefix ~ '"' ~ col ~ '" IS NOT NULL THEN LEFT(' ~ table_prefix ~ '"' ~ col ~ '", 3) || \'XX\' ELSE NULL END as "' ~ col ~ '_masked"') %}
                {% elif 'medicaid' in col.lower() and 'number' in col.lower() %}
                    {% do masked_columns.append("'REDACTED' as \"" ~ col ~ "_masked\"") %}
                {% elif 'name' in col.lower() or 'surname' in col.lower() %}
                    {% do masked_columns.append("'REDACTED' as \"" ~ col ~ "_masked\"") %}
                {% elif 'unique_id' in col.lower() %}
                    {% do masked_columns.append("'REDACTED' as \"" ~ col ~ "_masked\"") %}
                {% elif 'dob' in col.lower() or 'birth' in col.lower() %}
                    {% do masked_columns.append("'REDACTED' as \"" ~ col ~ "_masked\"") %}
                {% else %}
                    {% do masked_columns.append("'REDACTED' as \"" ~ col ~ "_masked\"") %}
                {% endif %}
            {% else %}
                -- Non-PII fields - keep as is
                {% do masked_columns.append(table_prefix ~ '"' ~ col ~ '"') %}
            {% endif %}
        {% endfor %}
        
        {{ return(masked_columns) }}
    {% else %}
        {{ return([]) }}
    {% endif %}
{% endmacro %}