#!/usr/bin/env python3
# ABOUTME: Consolidated model generation script for dbt questionnaire models.
# ABOUTME: Generates both named readable models and anonymized models.

import argparse
import csv
import os
from pathlib import Path


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
# Anonymized Model Generation
# =============================================================================

ANON_TEMPLATE = """{{{{
    config(
        materialized='view'
    )
}}}}

-- Anonymized view for {table_name}
-- Uses questionnaire_metadata.anon column to mask PII fields

{{{{ create_anonymized_qr_view('{table_name}', []) }}}}"""


def generate_anon_model(table_name):
    """Generate an anonymization model that uses the create_anonymized_qr_view macro."""
    return ANON_TEMPLATE.format(table_name=table_name)


def generate_anon_models():
    """Generate anonymization models for all questionnaire tables."""
    base_dir = Path(__file__).parent
    metadata_path = base_dir / 'seeds' / 'questionnaire_metadata.csv'
    output_dir = base_dir / 'models' / 'marts' / 'qr_anon'
    output_dir.mkdir(parents=True, exist_ok=True)

    # Read metadata to get list of tables
    tables = set()
    with open(metadata_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            table = row['table']
            if table.startswith('qr_'):
                tables.add(table)

    # Generate models
    generated_count = 0
    for table_name in sorted(tables):
        model_content = generate_anon_model(table_name)

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
