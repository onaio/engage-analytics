#!/usr/bin/env python3
import csv
import os
import re

# Read the discovered questionnaires CSV to get the file name mappings
discovered_file = '/Users/mberg/github/engage-analytics/dbt/discovered_questionnaires.csv'

# Template for readable models
template = """-- depends_on: {{{{ ref('int_qr_tags') }}}}
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

# Create models directory if it doesn't exist
models_dir = "/Users/mberg/github/engage-analytics/dbt/models/marts/qr_named"
os.makedirs(models_dir, exist_ok=True)

# Read the CSV and create mappings
questionnaires = []
with open(discovered_file, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row['id'] and row['path']:
            # Extract the file name without extension
            file_name = os.path.basename(row['path']).replace('.json', '')
            
            # Clean up the file name to make it SQL-friendly
            # Remove 'ipc-session-' prefix if it exists to shorten names
            clean_name = file_name.replace('ipc-session-', 's')
            # Replace hyphens with underscores
            clean_name = clean_name.replace('-', '_')
            
            # Build questionnaire ID
            if row['id'].startswith('ipc-'):
                # Remove 'ipc-' prefix for numeric IDs
                qid = row['id'].replace('ipc-', '')
            else:
                qid = row['id']
            
            questionnaires.append({
                'qid': qid,
                'file_name': f"qr_{clean_name}",
                'description': row['title'],
                'source_file': row['path']
            })

# Also handle questionnaires that use different ID formats
special_mappings = [
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

# Add special mappings
for mapping in special_mappings:
    # Check if this questionnaire is already in the list
    if not any(q['qid'] == mapping['qid'] for q in questionnaires):
        questionnaires.append(mapping)

# Generate model files
created_files = []
for q in questionnaires:
    filename = f"{q['file_name']}.sql"
    filepath = os.path.join(models_dir, filename)
    
    content = template.format(
        qid=q['qid'],
        description=q['description'],
        source_file=q['source_file']
    )
    
    with open(filepath, 'w') as f:
        f.write(content)
    
    created_files.append(filename)
    print(f"Created: {filename}")

print(f"\nGenerated {len(created_files)} named readable models in {models_dir}")

# Create a summary file
summary_file = os.path.join(models_dir, "_model_mappings.md")
with open(summary_file, 'w') as f:
    f.write("# Questionnaire Model Mappings\n\n")
    f.write("| Model Name | Questionnaire ID | Description | Source File |\n")
    f.write("|------------|------------------|-------------|-------------|\n")
    for q in sorted(questionnaires, key=lambda x: x['file_name']):
        f.write(f"| {q['file_name']} | {q['qid']} | {q['description']} | {q['source_file']} |\n")
    
print(f"Created summary file: {summary_file}")