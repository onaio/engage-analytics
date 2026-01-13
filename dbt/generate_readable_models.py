#!/usr/bin/env python3
import os

# Mapping of questionnaire IDs to names and descriptions
questionnaires = [
    ("221", "registration", "Add Family Member Registration"),
    ("55", "ipc_s1", "IPC Session 1"),
    ("58", "ipc_s2", "IPC Session 2"), 
    ("23451", "confidential", "Discuss Confidentiality"),
    ("204", "pcl5_s1", "Short-Form PCL-5-8 (IPC Session 1)"),
    ("104455", "spi_1", "SPI Subform 1"),
    ("202", "gad7_s1", "GAD-7 (IPC Session 1)"),
    ("208", "ipc_s3", "IPC Session 3"),
    ("212", "sbirt", "SBIRT"),
    ("104453", "spi_2", "SPI Subform 2"),
    ("104394", "spi_3", "SPI Subform 4"),
    ("1613532", "mental_wellness", "Mental Wellness Tool"),
    ("1613531", "demographic", "Sociodemographic Survey"),
    ("104454", "spi_4", "SPI Subform 3"),
    ("57", "phq9_s2", "PHQ-9 (IPC Session 2)"),
    ("56", "pcl5_s2", "Short-Form PCL-5-8 (IPC Session 2)"),
    ("63", "ipc_s4", "IPC Session 4"),
    ("205", "gad7_s2", "GAD-7 (IPC Session 2)"),
    ("60", "pcl5_s3", "Short-Form PCL-5-8 (IPC Session 3)"),
    ("61", "phq9_s3", "PHQ-9 (IPC Session 3)"),
    ("59", "gad7_s3", "GAD-7 (IPC Session 3)"),
    ("210", "pcl5_s4", "Short-Form PCL-5-8 (IPC Session 4)"),
    ("203", "mood_s1", "Mood Rating (IPC Session 1)"),
    ("62", "gad7_s4", "GAD-7 (IPC Session 4)"),
    ("211", "phq9_s4", "PHQ-9 (IPC Session 4)"),
    ("69", "remove_family", "eCBIS Remove Family Form"),
    ("207", "mood_s3", "Mood Rating (IPC Session 3)"),
    ("206", "mood_s2", "Mood Rating (IPC Session 2)"),
    ("209", "mood_s4", "Mood Rating (IPC Session 4)"),
]

# Template for readable models
template = """-- depends_on: {{{{ ref('int_qr_tags') }}}}
-- depends_on: {{{{ ref('int_qr_header') }}}}
-- depends_on: {{{{ ref('questionnaire_metadata') }}}}
{{{{ config(materialized='view') }}}}
-- {description} with readable column names
-- Questionnaire ID: {qid}
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
models_dir = "/Users/mberg/github/engage-analytics/dbt/models/marts/qr_readable"
os.makedirs(models_dir, exist_ok=True)

# Generate model files
for qid, short_name, description in questionnaires:
    filename = f"qr_{short_name}_readable.sql"
    filepath = os.path.join(models_dir, filename)
    
    content = template.format(
        qid=qid,
        description=description
    )
    
    with open(filepath, 'w') as f:
        f.write(content)
    
    print(f"Created: {filename}")

print(f"\nGenerated {len(questionnaires)} readable models in {models_dir}")