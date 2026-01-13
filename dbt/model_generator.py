#!/usr/bin/env python3
# ABOUTME: Consolidated model generation script for dbt questionnaire models.
# ABOUTME: Generates both named readable models and anonymized models.

import argparse
import csv
import os
import re
from pathlib import Path

import psycopg2


# =============================================================================
# Named Model Generation (from generate_named_readable_models.py)
# =============================================================================

NAMED_TEMPLATE = """-- depends_on: {{{{ ref('int_qr_tags') }}}}
-- depends_on: {{{{ ref('int_qr_header') }}}}
-- depends_on: {{{{ ref('questionnaire_metadata') }}}}
{{{{ config(materialized='view') }}}}
-- {description} with readable column names
-- Questionnaire ID: {qid}
-- Source file: {source_file}
{{% set identifiers = ["Questionnaire/{qid}"] %}}
{{% if identifiers|length == 0 %}}
-- No identifiers discovered for this Questionnaire; creating empty selectable view
select null::text as qr_id, null::text as questionnaire_id, null::text as subject_patient_id,
       null::text as encounter_id, null::text as author_practitioner_id
where false
{{% else %}}
{{{{ build_qr_wide_readable(identifiers, this.name) }}}}
{{% endif %}}"""

SPECIAL_MAPPINGS = [
    {"qid": "q-financial-wellness-tool", "file_name": "qr_financial_wellness_tool",
     "description": "Financial Wellness Tool", "source_file": "questionnaire/assessments/financial-wellness-tool.json"},
    {"qid": "q-planning-next-steps", "file_name": "qr_planning_next_steps",
     "description": "Planning Next Steps", "source_file": "questionnaire/assessments/planning-next-steps.json"},
    {"qid": "q-common-mental-health-symptoms", "file_name": "qr_common_mental_health_symptoms",
     "description": "Common Mental Health Symptoms", "source_file": "questionnaire/assessments/common-mental-health-symptoms.json"},
    {"qid": "1-month-follow-up", "file_name": "qr_1_month_follow_up",
     "description": "1 Month Follow Up", "source_file": "questionnaire/1-month-follow-up.json"},
    {"qid": "mw-tool-ipc-session-4", "file_name": "qr_mw_tool_ipc_session_4",
     "description": "Mental Wellness Tool Session 4", "source_file": "questionnaire/assessments/mw-tool-ipc-session-4.json"}
]


def generate_named_models():
    """Generate dbt models with semantic naming based on discovered questionnaires."""
    base_dir = Path(__file__).parent
    discovered_file = base_dir / 'discovered_questionnaires.csv'
    models_dir = base_dir / 'models' / 'marts' / 'qr_named'
    models_dir.mkdir(parents=True, exist_ok=True)

    questionnaires = []

    # Read discovered questionnaires
    with open(discovered_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['id'] and row['path']:
                file_name = os.path.basename(row['path']).replace('.json', '')
                clean_name = file_name.replace('ipc-session-', 's').replace('-', '_')

                qid = row['id'].replace('ipc-', '') if row['id'].startswith('ipc-') else row['id']

                questionnaires.append({
                    'qid': qid,
                    'file_name': f"qr_{clean_name}",
                    'description': row['title'],
                    'source_file': row['path']
                })

    # Add special mappings
    for mapping in SPECIAL_MAPPINGS:
        if not any(q['qid'] == mapping['qid'] for q in questionnaires):
            questionnaires.append(mapping)

    # Generate model files
    created_files = []
    for q in questionnaires:
        filename = f"{q['file_name']}.sql"
        filepath = models_dir / filename

        content = NAMED_TEMPLATE.format(
            qid=q['qid'],
            description=q['description'],
            source_file=q['source_file']
        )

        with open(filepath, 'w') as f:
            f.write(content)

        created_files.append(filename)
        print(f"Created: {filename}")

    print(f"\nGenerated {len(created_files)} named readable models in {models_dir}")

    # Create summary file
    summary_file = models_dir / "_model_mappings.md"
    with open(summary_file, 'w') as f:
        f.write("# Questionnaire Model Mappings\n\n")
        f.write("| Model Name | Questionnaire ID | Description | Source File |\n")
        f.write("|------------|------------------|-------------|-------------|\n")
        for q in sorted(questionnaires, key=lambda x: x['file_name']):
            f.write(f"| {q['file_name']} | {q['qid']} | {q['description']} | {q['source_file']} |\n")

    print(f"Created summary file: {summary_file}")
    return created_files


# =============================================================================
# Anonymized Model Generation (from generate_anon_models_v2.py)
# =============================================================================

def get_db_connection():
    """Get database connection using environment variables."""
    return psycopg2.connect(
        host=os.environ.get('DBT_HOST', 'localhost'),
        database=os.environ.get('DBT_DATABASE', 'airbyte'),
        user=os.environ.get('DBT_USER', 'postgres'),
        password=os.environ.get('DBT_PASSWORD', 'knox48'),
        port=os.environ.get('DBT_PORT', '5432')
    )


def get_actual_columns(table_name):
    """Get the actual column names from the database table."""
    conn = get_db_connection()
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


def generate_anon_model(table_name, metadata_columns):
    """Generate an anonymization model for a single table using actual column names."""

    try:
        actual_columns = get_actual_columns(table_name)
    except Exception as e:
        print(f"Warning: Could not get columns for {table_name}: {e}")
        actual_columns = []

    # Create mapping from column name to metadata
    metadata_map = {}
    for col in metadata_columns:
        metadata_map[col['column']] = col
        if 'short_name' in col and col['short_name']:
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
    pii_patterns = [
        'patient-name', 'patient-dob', 'patient-age',
        'patient-biological-sex', 'patient-gender-identity',
        'patient-how-you-think-of-yourself', 'patient-pronouns',
        'first_name', 'last_name', 'middle_name',
        'date_of_birth', 'medicaid_number', 'email_address',
        'physical_address', 'phone_number', 'zip_code',
        'unique_id', 'name_family', 'name_given', 'birthdate'
    ]

    for actual_col in actual_columns:
        if actual_col == 'LINK_ID_THAT_CONTAINS_ENCOUNTER_ID_AS_ANSWER':
            continue

        meta = metadata_map.get(actual_col)

        # Try without numeric suffix for duplicates
        if not meta and '_' in actual_col and actual_col[-1].isdigit():
            base_name = re.sub(r'_\d+$', '', actual_col)
            for key, val in metadata_map.items():
                if val.get('short_name', '').startswith(base_name):
                    meta = val
                    break

        # Determine if PII
        is_pii = False
        if meta:
            is_pii = meta.get('anon') == 'TRUE'
        else:
            for pattern in pii_patterns:
                if pattern.lower() in actual_col.lower().replace('_', '-'):
                    is_pii = True
                    break

        needs_quotes = ('-' in actual_col or '.' in actual_col or
                       ' ' in actual_col or (actual_col[0].isdigit() if actual_col else False))

        # Generate column line
        if actual_col == 'qr_id' and is_pii:
            column_lines.append(f"    MD5(COALESCE(qr_id, '')::text) as qr_id_hash")
        elif actual_col == 'subject_patient_id' and is_pii:
            column_lines.append(f"    MD5(COALESCE(subject_patient_id, '')::text) as subject_patient_id_hash")
        elif 'phone' in actual_col.lower() and is_pii:
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
            output_name = actual_col.replace('-', '_')
            column_lines.append(f"    'REDACTED' as {output_name}")
        else:
            if needs_quotes:
                output_name = actual_col.replace('-', '_')
                column_lines.append(f'    "{actual_col}" as {output_name}')
            else:
                column_lines.append(f'    {actual_col} as {actual_col}')

    column_lines.append('    CURRENT_TIMESTAMP as anonymized_at')

    model_content += ',\n'.join(column_lines)
    model_content += f"""

from {{{{ ref('{table_name}') }}}}"""

    return model_content


def generate_anon_models():
    """Generate anonymization models for all questionnaire tables."""
    base_dir = Path(__file__).parent
    metadata_path = base_dir / 'seeds' / 'questionnaire_metadata.csv'
    output_dir = base_dir / 'models' / 'marts' / 'qr_anon'
    output_dir.mkdir(parents=True, exist_ok=True)

    # Read metadata
    tables_data = {}
    with open(metadata_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            table = row['table']
            if table not in tables_data:
                tables_data[table] = []
            tables_data[table].append(row)

    # Generate models
    generated_count = 0
    for table_name, columns in tables_data.items():
        if table_name.startswith('qr_'):
            model_content = generate_anon_model(table_name, columns)

            output_file = output_dir / f"{table_name}_anon.sql"
            with open(output_file, 'w') as f:
                f.write(model_content)

            generated_count += 1
            print(f"Generated {output_file.name}")

    print(f"\nGenerated {generated_count} anonymization models in {output_dir}")
    return generated_count


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Generate dbt models for questionnaire responses'
    )
    subparsers = parser.add_subparsers(dest='command', help='Command to run')

    # Named models subcommand
    subparsers.add_parser('named', help='Generate named readable models (qr_named/)')

    # Anon models subcommand
    subparsers.add_parser('anon', help='Generate anonymized models (qr_anon/)')

    # All models subcommand
    subparsers.add_parser('all', help='Generate both named and anonymized models')

    args = parser.parse_args()

    if args.command == 'named':
        generate_named_models()
    elif args.command == 'anon':
        generate_anon_models()
    elif args.command == 'all':
        print("=" * 60)
        print("Generating named readable models...")
        print("=" * 60)
        generate_named_models()
        print()
        print("=" * 60)
        print("Generating anonymized models...")
        print("=" * 60)
        generate_anon_models()
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
