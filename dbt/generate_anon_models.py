#!/usr/bin/env python3
"""
Generate anonymization models for QR tables based on questionnaire_metadata.csv
"""
import csv
import os
from pathlib import Path

def generate_anon_model(table_name, columns):
    """Generate an anonymization model for a single table"""
    
    # Group columns by PII status
    pii_columns = [col for col in columns if col['anon'] == 'TRUE']
    non_pii_columns = [col for col in columns if col['anon'] == 'FALSE']
    
    model_content = f"""{{{{
    config(
        materialized='view'
    )
}}}}

-- Anonymized view for {table_name}
-- Automatically generated based on questionnaire_metadata.csv

select 
"""
    
    # Add columns with appropriate masking
    column_lines = []
    
    for col in columns:
        col_name = col['column']
        col_label = col['label']
        # Use short_name for both source column reference and output alias
        col_alias = col.get('short_name', col_name) if 'short_name' in col else col_name
        # For UUID-like columns, the source table now uses the short_name as the column name
        # Check if this is a UUID-like column
        import re
        is_uuid = bool(re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', col_name))
        source_col = col_alias if is_uuid else col_name
        
        is_pii = col['anon'] == 'TRUE'
        
        # Skip special columns that don't exist in the actual table
        if col_name in ['LINK_ID_THAT_CONTAINS_ENCOUNTER_ID_AS_ANSWER']:
            continue
            
        # Handle special cases
        if col_name == 'qr_id' and is_pii:
            column_lines.append(f"    MD5(COALESCE(qr_id, '')::text) as qr_id_hash")
        elif col_name == 'subject_patient_id' and is_pii:
            column_lines.append(f"    MD5(COALESCE(subject_patient_id, '')::text) as subject_patient_id_hash")
        elif 'phone' in source_col.lower() and is_pii:
            # Show last 4 digits of phone
            column_lines.append(f"""    CASE 
        WHEN {source_col} IS NOT NULL 
        THEN 'XXX-XXX-' || RIGHT({source_col}, 4)
        ELSE NULL
    END as {col_alias}""")
        elif 'zip' in source_col.lower() and is_pii:
            # Show first 3 digits of zip
            column_lines.append(f"""    CASE 
        WHEN {source_col} IS NOT NULL 
        THEN LEFT({source_col}, 3) || 'XX'
        ELSE NULL
    END as {col_alias}""")
        elif is_pii:
            # Fully redact other PII
            # Always use the alias for output column name
            column_lines.append(f"    'REDACTED' as {col_alias}")
        else:
            # Keep non-PII as-is
            # Check if we need quotes for the source column
            needs_quotes_source = ('-' in source_col or 
                           '.' in source_col or 
                           ' ' in source_col or
                           (source_col[0].isdigit() if source_col else False))
            
            # For non-PII columns, use the source column name (which may be the short_name for UUIDs)
            # No alias needed since output name matches source name
            if needs_quotes_source:
                column_lines.append(f'    "{source_col}" as {col_alias}')
            else:
                column_lines.append(f'    {source_col} as {col_alias}')
    
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