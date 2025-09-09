#!/usr/bin/env python3
"""
Generate anonymization models for QR tables that reference the actual column names 
from the base tables, regardless of how duplicates were handled
"""
import csv
import os
from pathlib import Path
import psycopg2
import re

def get_actual_columns(table_name):
    """Get the actual column names from the database table"""
    conn = psycopg2.connect(
        host=os.environ.get('DBT_HOST', 'localhost'),
        database=os.environ.get('DBT_DATABASE', 'airbyte'),
        user=os.environ.get('DBT_USER', 'postgres'),
        password=os.environ.get('DBT_PASSWORD', 'knox48')
    )
    cur = conn.cursor()
    
    cur.execute(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'engage_analytics_engage_analytics_mart'
        AND table_name = '{table_name}'
        ORDER BY ordinal_position
    """)
    
    columns = [row[0] for row in cur.fetchall()]
    conn.close()
    return columns

def is_uuid_like(text):
    """Check if text looks like a UUID"""
    return bool(re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', text))

def generate_anon_model(table_name, metadata_columns):
    """Generate an anonymization model for a single table using actual column names"""
    
    # Get the actual columns from the database
    try:
        actual_columns = get_actual_columns(table_name)
    except Exception as e:
        print(f"Warning: Could not get columns for {table_name}: {e}")
        actual_columns = []
    
    # Create a mapping from original column name to metadata
    metadata_map = {}
    for col in metadata_columns:
        # Map both the original column and short_name to the metadata
        metadata_map[col['column']] = col
        if 'short_name' in col and col['short_name']:
            # Also map by short_name for UUID columns
            metadata_map[col['short_name']] = col
    
    model_content = f"""{{{{
    config(
        materialized='view'
    )
}}}}

-- Anonymized view for {table_name}
-- Automatically generated based on questionnaire_metadata.csv

select 
"""
    
    column_lines = []
    
    for actual_col in actual_columns:
        # Skip if column doesn't exist in table
        if actual_col == 'LINK_ID_THAT_CONTAINS_ENCOUNTER_ID_AS_ANSWER':
            continue
            
        # Find metadata for this column
        # Try exact match first
        meta = metadata_map.get(actual_col)
        
        # If not found and looks like a duplicate suffix (e.g., please_specify_3)
        # try to match the base name
        if not meta and '_' in actual_col and actual_col[-1].isdigit():
            # Try without the numeric suffix
            base_name = re.sub(r'_\d+$', '', actual_col)
            # Find any metadata entry with this base name
            for key, val in metadata_map.items():
                if val.get('short_name', '').startswith(base_name):
                    meta = val
                    break
        
        # Determine if this is PII
        is_pii = False
        if meta:
            is_pii = meta.get('anon') == 'TRUE'
        else:
            # Check common PII patterns if no metadata found
            pii_patterns = [
                'patient-name', 'patient-dob', 'patient-age', 
                'patient-biological-sex', 'patient-gender-identity',
                'patient-how-you-think-of-yourself', 'patient-pronouns',
                'first_name', 'last_name', 'middle_name',
                'date_of_birth', 'medicaid_number', 'email_address',
                'physical_address', 'phone_number', 'zip_code',
                'unique_id', 'name_family', 'name_given', 'birthdate'
            ]
            
            for pattern in pii_patterns:
                if pattern.lower() in actual_col.lower().replace('_', '-'):
                    is_pii = True
                    break
        
        # Generate the column line
        needs_quotes = ('-' in actual_col or '.' in actual_col or 
                       ' ' in actual_col or (actual_col[0].isdigit() if actual_col else False))
        
        # Handle special cases
        if actual_col == 'qr_id' and is_pii:
            column_lines.append(f"    MD5(COALESCE(qr_id, '')::text) as qr_id_hash")
        elif actual_col == 'subject_patient_id' and is_pii:
            column_lines.append(f"    MD5(COALESCE(subject_patient_id, '')::text) as subject_patient_id_hash")
        elif 'phone' in actual_col.lower() and is_pii:
            # Show last 4 digits of phone
            if needs_quotes:
                column_lines.append(f"""    CASE 
        WHEN "{actual_col}" IS NOT NULL 
        THEN 'XXX-XXX-' || RIGHT("{actual_col}", 4)
        ELSE NULL
    END as {actual_col.replace('-', '_')}""")
            else:
                column_lines.append(f"""    CASE 
        WHEN {actual_col} IS NOT NULL 
        THEN 'XXX-XXX-' || RIGHT({actual_col}, 4)
        ELSE NULL
    END as {actual_col}""")
        elif 'zip' in actual_col.lower() and is_pii:
            # Show first 3 digits of zip
            if needs_quotes:
                column_lines.append(f"""    CASE 
        WHEN "{actual_col}" IS NOT NULL 
        THEN LEFT("{actual_col}", 3) || 'XX'
        ELSE NULL
    END as {actual_col.replace('-', '_')}""")
            else:
                column_lines.append(f"""    CASE 
        WHEN {actual_col} IS NOT NULL 
        THEN LEFT({actual_col}, 3) || 'XX'
        ELSE NULL
    END as {actual_col}""")
        elif is_pii:
            # Fully redact other PII
            output_name = actual_col.replace('-', '_')
            column_lines.append(f"    'REDACTED' as {output_name}")
        else:
            # Keep non-PII as-is
            if needs_quotes:
                output_name = actual_col.replace('-', '_')
                column_lines.append(f'    "{actual_col}" as {output_name}')
            else:
                column_lines.append(f'    {actual_col} as {actual_col}')
    
    # Add anonymized_at timestamp
    column_lines.append('    CURRENT_TIMESTAMP as anonymized_at')
    
    model_content += ',\n'.join(column_lines)
    model_content += f"""

from {{{{ ref('{table_name}') }}}}"""
    
    return model_content

def main():
    # Read metadata
    metadata_path = '/Users/mberg/github/engage-analytics/dbt/seeds/questionnaire_metadata.csv'
    
    tables_data = {}
    with open(metadata_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            table = row['table']
            if table not in tables_data:
                tables_data[table] = []
            tables_data[table].append(row)
    
    # Create output directory
    output_dir = Path('/Users/mberg/github/engage-analytics/dbt/models/marts/qr_anon')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate models
    generated_count = 0
    for table_name, columns in tables_data.items():
        if table_name.startswith('qr_'):
            model_content = generate_anon_model(table_name, columns)
            
            # Write model file
            output_file = output_dir / f"{table_name}_anon.sql"
            with open(output_file, 'w') as f:
                f.write(model_content)
            
            generated_count += 1
            print(f"Generated {output_file}")
    
    print(f"\nGenerated {generated_count} anonymization models")

if __name__ == '__main__':
    main()