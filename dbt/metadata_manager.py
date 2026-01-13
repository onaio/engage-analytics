#!/usr/bin/env python3
# ABOUTME: Consolidated metadata management script for questionnaire metadata.
# ABOUTME: Combines extraction, fixing, and enrichment operations.

import argparse
import csv
import glob
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

import psycopg2


def get_db_connection():
    """Get database connection using environment variables."""
    return psycopg2.connect(
        host=os.environ.get('DBT_HOST', 'localhost'),
        database=os.environ.get('DBT_DATABASE', 'airbyte'),
        user=os.environ.get('DBT_USER', 'postgres'),
        password=os.environ.get('DBT_PASSWORD', 'knox48'),
        port=os.environ.get('DBT_PORT', '5432')
    )


def get_base_dir():
    """Get the base directory of the dbt project."""
    return Path(__file__).parent


# =============================================================================
# Extract Question Text (from extract_questionnaire_text.py)
# =============================================================================

def clean_text(text):
    """Clean up question text for use as a label."""
    if not text:
        return ""
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\[[\w-]+\]', '', text)
    text = ' '.join(text.split())
    if len(text) > 100:
        text = text[:97] + "..."
    return text.strip()


def extract_items_recursive(items, linkid_map, parent_context=""):
    """Recursively extract linkIds and text from questionnaire items."""
    if not items:
        return

    for item in items:
        if not isinstance(item, dict):
            continue

        linkid = item.get('linkId', '')
        text = item.get('text', '')
        item_type = item.get('type', '')

        clean = clean_text(text)

        if linkid and clean:
            if parent_context and item_type not in ['group', 'display']:
                full_text = f"{parent_context} - {clean}" if parent_context != clean else clean
            else:
                full_text = clean
            linkid_map[linkid] = full_text
        elif linkid and not clean:
            if item_type == 'group':
                linkid_map[linkid] = parent_context or "Group"
            elif item_type == 'display':
                linkid_map[linkid] = "Display Text"
            elif parent_context:
                linkid_map[linkid] = parent_context

        answer_option = item.get('answerOption', [])
        for answer in answer_option:
            if isinstance(answer, dict):
                value_coding = answer.get('valueCoding', {})
                if isinstance(value_coding, dict):
                    code = value_coding.get('code', '')
                    display = value_coding.get('display', '')
                    if code and display:
                        linkid_map[code] = display

        nested_items = item.get('item', [])
        if nested_items:
            new_context = clean if clean and item_type == 'group' else parent_context
            extract_items_recursive(nested_items, linkid_map, new_context)


def cmd_extract_text():
    """Extract question text from questionnaire JSON files."""
    questionnaire_dir = "/Users/mberg/github/engage-analytics/questionnaire"
    json_files = glob.glob(f"{questionnaire_dir}/**/*.json", recursive=True)
    json_files = [f for f in json_files if 'obsolete' not in f]

    print(f"Found {len(json_files)} questionnaire files to process")

    all_linkids = {}

    for filepath in json_files:
        print(f"Processing {Path(filepath).name}...")
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            title = data.get('title', Path(filepath).stem)
            items = data.get('item', [])
            linkid_map = {}
            extract_items_recursive(items, linkid_map, title)

            for linkid, text in linkid_map.items():
                if linkid in all_linkids:
                    if len(text) > len(all_linkids[linkid]):
                        all_linkids[linkid] = text
                else:
                    all_linkids[linkid] = text
        except Exception as e:
            print(f"Error processing {filepath}: {e}")

    output_file = get_base_dir() / "linkid_question_mapping.json"
    with open(output_file, 'w') as f:
        json.dump(all_linkids, f, indent=2, sort_keys=True)

    print(f"\nExtracted {len(all_linkids)} unique linkId-to-question mappings")
    print(f"Saved to {output_file}")


# =============================================================================
# Find Unmapped UUID Fields (from find_all_uuid_fields.py)
# =============================================================================

def cmd_find_unmapped():
    """Find UUID-formatted linkIds not yet in metadata."""
    conn = get_db_connection()
    cur = conn.cursor()

    query = """
    SELECT DISTINCT
        a.questionnaire_id,
        a.linkid,
        SUBSTRING(MAX(a.answer_value_text), 1, 100) as sample_value,
        COUNT(DISTINCT a.qr_id) as response_count
    FROM engage_analytics_engage_analytics_int.int_qr_answers_long a
    WHERE a.linkid ~ '^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$'
    GROUP BY a.questionnaire_id, a.linkid
    ORDER BY a.questionnaire_id, a.linkid
    """

    cur.execute(query)
    uuid_fields = cur.fetchall()

    print(f"Found {len(uuid_fields)} UUID field instances across questionnaires")

    metadata_file = get_base_dir() / 'seeds' / 'questionnaire_metadata.csv'
    existing_linkids = set()
    if metadata_file.exists():
        with open(metadata_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('linkid'):
                    existing_linkids.add(row['linkid'])

    unmapped = []
    for qid, linkid, sample, count in uuid_fields:
        if linkid not in existing_linkids:
            unmapped.append({
                'questionnaire_id': qid,
                'linkid': linkid,
                'sample_value': sample,
                'response_count': count
            })

    print(f"Found {len(unmapped)} unmapped UUID fields")

    output_file = get_base_dir() / 'unmapped_uuid_fields.csv'
    with open(output_file, 'w', newline='') as f:
        fieldnames = ['questionnaire_id', 'linkid', 'sample_value', 'response_count']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(unmapped)

    print(f"Saved unmapped fields to {output_file}")

    cur.close()
    conn.close()


# =============================================================================
# Fix Duplicate Short Names (from fix_duplicate_short_names.py)
# =============================================================================

def cmd_fix_duplicates():
    """Fix duplicate short_name values by adding numeric suffixes."""
    metadata_path = get_base_dir() / 'seeds' / 'questionnaire_metadata.csv'

    with open(metadata_path, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames

    print(f"Read {len(rows)} metadata rows")

    if 'short_name' not in fieldnames or 'table' not in fieldnames:
        print("Error: Metadata file missing required columns (table, short_name)")
        return

    table_short_names = defaultdict(lambda: defaultdict(list))

    for i, row in enumerate(rows):
        table = row['table']
        short_name = row['short_name']
        table_short_names[table][short_name].append(i)

    duplicate_count = 0
    for table, short_names in table_short_names.items():
        for short_name, indices in short_names.items():
            if len(indices) > 1:
                duplicate_count += len(indices) - 1
                print(f"Table {table}: '{short_name}' appears {len(indices)} times")

    print(f"\nFound {duplicate_count} duplicate short names across all tables")

    for table, short_names in table_short_names.items():
        for short_name, indices in short_names.items():
            if len(indices) > 1:
                for j, idx in enumerate(indices[1:], start=2):
                    new_short_name = f"{short_name}_{j}"
                    if len(new_short_name) > 30:
                        base = short_name[:30-len(f"_{j}")]
                        new_short_name = f"{base}_{j}"
                    rows[idx]['short_name'] = new_short_name

    with open(metadata_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nFixed {duplicate_count} duplicate short names")
    print(f"Updated {metadata_path}")


# =============================================================================
# Enrich Metadata (from update_metadata_with_questions.py)
# =============================================================================

def create_short_name(column_name, label, table_name=''):
    """Create a shortened column name for use in marts based on the label."""
    system_fields = ['qr_id', 'questionnaire_id', 'subject_patient_id',
                     'encounter_id', 'author_practitioner_id',
                     'practitioner_location_id', 'practitioner_organization_id',
                     'practitioner_id', 'practitioner_careteam_id',
                     'application_version']
    if column_name in system_fields:
        return column_name

    if column_name.startswith('patient-'):
        return column_name.replace('-', '_')

    if column_name.startswith('demographic_'):
        short = column_name.replace('demographic_', 'demo_')
        short = short.replace('what_is_your_', '').replace('what_is_the_', '')
        short = short.replace('please_specify', 'specify')
        return short[:30] if len(short) > 30 else short

    uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    if re.match(uuid_pattern, column_name, re.IGNORECASE) or label:
        if label and len(label) > 3:
            clean_label = label
            prefixes_to_remove = [
                r'Financial Wellness Tool - ', r'Sociodemographic Survey - ',
                r'SBIRT - ', r'PHQ-9 \(IPC Session \d+\) - ',
                r'GAD-7 \(IPC Session \d+\) - ', r'Short-Form PCL-5 \(IPC Session \d+\) - ',
                r'IPC Session \d+ - ', r'Mood Rating \(IPC Session \d+\) - ',
                r'SPI Subform \d+ - ',
            ]
            for prefix in prefixes_to_remove:
                clean_label = re.sub(f'^{prefix}', '', clean_label)

            # Extract meaningful words
            words = re.findall(r'\b[A-Za-z]+\b', clean_label)
            stopwords = ['the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at',
                        'to', 'for', 'of', 'with', 'by', 'from', 'up', 'about',
                        'into', 'through', 'during', 'how', 'when', 'where',
                        'what', 'which', 'who', 'whom', 'this', 'that', 'these',
                        'those', 'then', 'than', 'are', 'was', 'were', 'been',
                        'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would',
                        'could', 'should', 'may', 'might', 'must', 'can', 'much',
                        'often', 'past', 'month', 'months', 'year', 'years', 'time']
            meaningful_words = [w.lower() for w in words if w.lower() not in stopwords and len(w) > 2]

            if meaningful_words:
                short = '_'.join(meaningful_words[:3])
                if 'sbirt' in table_name and len(short) < 20:
                    short = 'sbirt_' + short
                elif 'financial' in table_name and len(short) < 20:
                    short = 'fin_' + short
                elif 'phq' in table_name and len(short) < 25:
                    short = 'phq_' + short
                elif 'gad' in table_name and len(short) < 25:
                    short = 'gad_' + short
                return short[:30] if len(short) > 30 else short

        return f"q_{column_name[:8].replace('-', '')}"

    if '.' in column_name:
        return column_name.replace('.', '_')
    if '-' in column_name:
        if len(column_name) <= 30:
            return column_name.replace('-', '_')
        parts = column_name.split('-')
        return '_'.join(parts[:3])[:30]

    return column_name[:30] if len(column_name) > 30 else column_name


def column_to_label(column_name, table_name, linkid_map):
    """Convert column names to human-readable labels using actual question text."""
    special_cases = {
        'qr_id': 'Questionnaire Response ID',
        'questionnaire_id': 'Questionnaire ID',
        'subject_patient_id': 'Subject Patient ID',
        'encounter_id': 'Encounter ID',
        'author_practitioner_id': 'Author Practitioner ID',
        'practitioner_location_id': 'Practitioner Location ID',
        'practitioner_organization_id': 'Practitioner Organization ID',
        'practitioner_id': 'Practitioner ID',
        'practitioner_careteam_id': 'Practitioner Care Team ID',
        'application_version': 'Application Version',
        'patient-age': 'Patient Age',
        'patient-biological-sex': 'Patient Biological Sex',
        'patient-dob': 'Patient Date of Birth',
        'patient-gender-identity': 'Patient Gender Identity',
        'patient-name': 'Patient Name',
        'patient-pronouns': 'Patient Pronouns',
    }

    if column_name in special_cases:
        return special_cases[column_name]

    uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    if re.match(uuid_pattern, column_name, re.IGNORECASE):
        question_text = linkid_map.get(column_name, '')
        if question_text and len(question_text) > 3:
            text = question_text.strip()
            text = re.sub(r'^[,.\s]+', '', text)
            return text[:77] + "..." if len(text) > 80 else text
        return 'Survey Response'

    if '.' in column_name:
        question_text = linkid_map.get(column_name, '')
        if question_text and len(question_text) > 3:
            text = question_text.strip()
            return text[:77] + "..." if len(text) > 80 else text
        return f'Form Question {column_name}'

    if column_name.startswith('demographic_'):
        label = column_name.replace('demographic_', '').replace('_', ' ')
        return ' '.join(word.capitalize() for word in label.split())

    if '-' in column_name:
        question_text = linkid_map.get(column_name, '')
        if question_text and len(question_text) > 3:
            text = question_text.strip()
            return text[:77] + "..." if len(text) > 80 else text
        return column_name.replace('-', ' ').title()

    return column_name.replace('_', ' ').title()


def get_questionnaire_title(table_name):
    """Get questionnaire title from table name."""
    title_mapping = {
        'qr_registration_info': 'Add Family Member Registration',
        'qr_sociodemographic_survey': 'Sociodemographic Survey',
        'qr_spi_subform_1': 'SPI Subform 1',
        'qr_spi_subform_2': 'SPI Subform 2',
        'qr_spi_subform_3': 'SPI Subform 3',
        'qr_spi_subform_4': 'SPI Subform 4',
        'qr_sbirt': 'SBIRT',
        'qr_1_month_follow_up': '1 month follow up',
        'qr_common_mental_health_symptoms': 'Common Mental Health Symptoms',
        'qr_confidential_s1': 'Discuss Confidentiality',
        'qr_financial_wellness_tool': 'Financial Wellness Tool',
        'qr_planning_next_steps': 'Planning Next Steps',
        'qr_mw_tool': 'Mental Wellness Tool',
        'qr_mw_tool_ipc_session_4': 'Mental Wellness Tool Session 4',
        'qr_remove_patient': 'eCBIS Remove Family Form',
    }

    if table_name in title_mapping:
        return title_mapping[table_name]

    # Handle session-based questionnaires
    session_patterns = [
        ('gad', 'GAD-7'),
        ('phq', 'PHQ-9'),
        ('mood_rating', 'Mood Rating'),
        ('pcl', 'Short-Form PCL-5-8'),
        ('start_ipc', 'IPC Session'),
    ]

    for pattern, prefix in session_patterns:
        if pattern in table_name:
            for i in range(1, 5):
                if f's{i}' in table_name:
                    return f'{prefix} (IPC Session {i})'

    return ''


def cmd_enrich():
    """Enrich metadata with labels, short names, and PII flags."""
    base_dir = get_base_dir()

    # Load linkId to question mapping
    mapping_file = base_dir / 'linkid_question_mapping.json'
    if not mapping_file.exists():
        print("Error: linkid_question_mapping.json not found. Run extract-text first.")
        return

    with open(mapping_file, 'r') as f:
        linkid_map = json.load(f)

    print(f"Loaded {len(linkid_map)} linkId mappings")

    # Connect to database
    conn = get_db_connection()
    cur = conn.cursor()

    # Get all qr_* tables
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'engage_analytics_engage_analytics_mart'
        AND table_name LIKE 'qr_%'
        AND table_name NOT LIKE '%_anon'
        ORDER BY table_name
    """)
    tables = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()

    print(f"Found {len(tables)} questionnaire tables")

    # PII patterns
    pii_patterns = [
        'patient-name', 'patient-dob', 'patient-age',
        'patient-biological-sex', 'patient-gender-identity',
        'first_name', 'last_name', 'middle_name',
        'date_of_birth', 'medicaid_number', 'email_address',
        'physical_address', 'phone_number', 'zip_code',
        'unique_id', 'name_family', 'name_given', 'birthdate'
    ]

    all_columns = []

    for table_name in tables:
        try:
            conn = get_db_connection()
            cur = conn.cursor()

            # Get columns
            cur.execute(f"SELECT * FROM engage_analytics_engage_analytics_mart.{table_name} LIMIT 1")
            actual_columns = [desc[0] for desc in cur.description]

            # Get data types
            cur.execute(f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'engage_analytics_engage_analytics_mart'
                AND table_name = '{table_name}'
            """)
            column_types = {row[0]: row[1] for row in cur.fetchall()}

            cur.close()
            conn.close()

            for col_name in actual_columns:
                if col_name == 'LINK_ID_THAT_CONTAINS_ENCOUNTER_ID_AS_ANSWER':
                    continue

                data_type = column_types.get(col_name, 'text')

                # Check PII
                is_pii = any(p.lower() in col_name.lower().replace('_', '-') for p in pii_patterns)

                # Determine source
                system_fields = ['qr_id', 'questionnaire_id', 'subject_patient_id',
                               'encounter_id', 'author_practitioner_id',
                               'practitioner_location_id', 'practitioner_organization_id',
                               'practitioner_id', 'practitioner_careteam_id',
                               'application_version']
                if col_name in system_fields:
                    source = 'system'
                elif col_name.startswith('patient-'):
                    source = 'generated'
                else:
                    source = 'questionnaire'

                label = column_to_label(col_name, table_name, linkid_map)
                short_name = create_short_name(col_name, label, table_name)

                all_columns.append({
                    'table': table_name,
                    'column': col_name,
                    'linkid': col_name if re.match(r'^[0-9a-f]{8}-', col_name) else '',
                    'short_name': short_name,
                    'label': label,
                    'data_type': data_type,
                    'questionnaire_title': get_questionnaire_title(table_name),
                    'source': source,
                    'anon': 'TRUE' if is_pii else 'FALSE'
                })

        except Exception as e:
            print(f"Warning: Could not process table {table_name}: {e}")

    # Write to CSV
    output_file = base_dir / 'seeds' / 'questionnaire_metadata.csv'
    with open(output_file, 'w', newline='') as f:
        fieldnames = ['table', 'column', 'linkid', 'short_name', 'label',
                     'data_type', 'questionnaire_title', 'source', 'anon']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_columns)

    print(f"\nEnriched metadata with {len(all_columns)} fields")
    print(f"Tables processed: {len(tables)}")
    print(f"PII fields marked: {sum(1 for c in all_columns if c['anon'] == 'TRUE')}")
    print(f"Saved to {output_file}")


# =============================================================================
# Add Common Fields (from add_common_fields_to_metadata.py)
# =============================================================================

COMMON_FIELDS = {
    'cd8e3d6d-e9ff-458d-d122-57070bebffaf': {'text': 'Date of Birth', 'alias': 'date_of_birth'},
    'b5bc7f80-4a0c-486c-e5eb-32c750036f94': {'text': 'Age', 'alias': 'age'},
    'calculated-month': {'text': 'Birth Month', 'alias': 'birth_month'},
    'calculated-year': {'text': 'Age in Years', 'alias': 'age_years'},
    'LINK_ID_THAT_CONTAINS_ENCOUNTER_ID_AS_ANSWER': {'text': 'Encounter Reference', 'alias': 'encounter_reference'},
}


def cmd_add_common():
    """Add common fields (DOB, Age, etc.) to metadata."""
    conn = get_db_connection()
    cur = conn.cursor()

    # Find which questionnaires use which common fields
    questionnaire_fields = defaultdict(set)
    for linkid in COMMON_FIELDS.keys():
        cur.execute("""
            SELECT DISTINCT questionnaire_id
            FROM engage_analytics_engage_analytics_int.int_qr_answers_long
            WHERE linkid = %s
        """, (linkid,))
        for row in cur.fetchall():
            questionnaire_fields[row[0]].add(linkid)

    cur.close()
    conn.close()

    # Read existing metadata
    metadata_file = get_base_dir() / 'seeds' / 'questionnaire_metadata.csv'
    existing_data = []
    existing_keys = set()

    with open(metadata_file, 'r') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            existing_data.append(row)
            existing_keys.add((row.get('table', ''), row.get('linkid', '')))

    # Add new entries
    new_count = 0
    for qid, linkids in questionnaire_fields.items():
        for linkid in linkids:
            # Convert questionnaire_id to table name
            table_name = 'qr_' + qid.replace('Questionnaire/', '').replace('-', '_')

            if (table_name, linkid) not in existing_keys:
                field_info = COMMON_FIELDS[linkid]
                new_entry = {
                    'table': table_name,
                    'column': linkid,
                    'linkid': linkid,
                    'short_name': field_info['alias'],
                    'label': field_info['text'],
                    'data_type': 'text',
                    'questionnaire_title': '',
                    'source': 'questionnaire',
                    'anon': 'FALSE'
                }
                existing_data.append(new_entry)
                new_count += 1

    # Write back
    with open(metadata_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(existing_data)

    print(f"Added {new_count} common field entries")
    print(f"Total metadata entries: {len(existing_data)}")


# =============================================================================
# Fix Hex LinkIds (from fix_hex_linkids.py + update_hex_aliases.py)
# =============================================================================

def cmd_fix_hex():
    """Handle hex-formatted linkIds in Questionnaire/55."""
    conn = get_db_connection()
    cur = conn.cursor()

    # Find hex-formatted linkIds in Questionnaire/55
    cur.execute("""
        SELECT DISTINCT linkid, MAX(answer_value_text) as sample_value
        FROM engage_analytics_engage_analytics_int.int_qr_answers_long
        WHERE questionnaire_id = 'Questionnaire/55'
        AND linkid ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
        GROUP BY linkid
    """)
    hex_fields = cur.fetchall()
    cur.close()
    conn.close()

    print(f"Found {len(hex_fields)} hex-formatted linkIds in Questionnaire/55")

    if not hex_fields:
        print("No hex fields to process")
        return

    # Read existing metadata
    metadata_file = get_base_dir() / 'seeds' / 'questionnaire_metadata.csv'
    existing_data = []
    existing_keys = set()

    with open(metadata_file, 'r') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            existing_data.append(row)
            existing_keys.add((row.get('table', ''), row.get('linkid', '')))

    # Add/update entries
    new_count = 0
    for linkid, sample in hex_fields:
        table_name = 'qr_start_ipc_s1'  # Questionnaire/55 is IPC Session 1

        if (table_name, linkid) not in existing_keys:
            # Create alias from sample value
            if sample:
                alias = re.sub(r'[^\w\s]', '', sample[:60])
                alias = re.sub(r'\s+', '_', alias).lower()[:40]
                alias = f"ipc_s1_{alias}" if alias else f"ipc_s1_field_{linkid[:8]}"
            else:
                alias = f"ipc_s1_field_{linkid[:8]}"

            new_entry = {
                'table': table_name,
                'column': linkid,
                'linkid': linkid,
                'short_name': alias[:63],
                'label': sample[:100] if sample else 'Field',
                'data_type': 'text',
                'questionnaire_title': 'IPC Session 1',
                'source': 'questionnaire',
                'anon': 'FALSE'
            }
            existing_data.append(new_entry)
            new_count += 1

    # Write back
    with open(metadata_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(existing_data)

    print(f"Added {new_count} hex field entries")
    print(f"Total metadata entries: {len(existing_data)}")


# =============================================================================
# Show Status
# =============================================================================

def cmd_status():
    """Show status of metadata and related files."""
    base_dir = get_base_dir()

    print("=" * 60)
    print("METADATA STATUS")
    print("=" * 60)

    # Check linkid_question_mapping.json
    mapping_file = base_dir / 'linkid_question_mapping.json'
    if mapping_file.exists():
        with open(mapping_file, 'r') as f:
            mappings = json.load(f)
        print(f"✓ linkid_question_mapping.json: {len(mappings)} mappings")
    else:
        print("✗ linkid_question_mapping.json: NOT FOUND")

    # Check unmapped_uuid_fields.csv
    unmapped_file = base_dir / 'unmapped_uuid_fields.csv'
    if unmapped_file.exists():
        with open(unmapped_file, 'r') as f:
            reader = csv.reader(f)
            count = sum(1 for _ in reader) - 1
        print(f"✓ unmapped_uuid_fields.csv: {count} unmapped fields")
    else:
        print("✗ unmapped_uuid_fields.csv: NOT FOUND")

    # Check questionnaire_metadata.csv
    metadata_file = base_dir / 'seeds' / 'questionnaire_metadata.csv'
    if metadata_file.exists():
        with open(metadata_file, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        tables = set(row.get('table', '') for row in rows)
        print(f"✓ questionnaire_metadata.csv: {len(rows)} rows, {len(tables)} tables")
    else:
        print("✗ questionnaire_metadata.csv: NOT FOUND")

    # Check discovered_questionnaires.csv
    discovered_file = base_dir / 'discovered_questionnaires.csv'
    if discovered_file.exists():
        with open(discovered_file, 'r') as f:
            reader = csv.reader(f)
            count = sum(1 for _ in reader) - 1
        print(f"✓ discovered_questionnaires.csv: {count} questionnaires")
    else:
        print("✗ discovered_questionnaires.csv: NOT FOUND")

    print("=" * 60)


# =============================================================================
# Full Refresh (run all steps in sequence)
# =============================================================================

def cmd_full_refresh():
    """Run all metadata operations in sequence."""
    print("=" * 60)
    print("STEP 1: Extracting question text from questionnaire files")
    print("=" * 60)
    cmd_extract_text()

    print()
    print("=" * 60)
    print("STEP 2: Enriching metadata with labels and PII flags")
    print("=" * 60)
    try:
        cmd_enrich()
    except Exception as e:
        print(f"Warning: Could not enrich metadata: {e}")

    print()
    print("=" * 60)
    print("STEP 3: Adding common fields (DOB, Age, etc.)")
    print("=" * 60)
    try:
        cmd_add_common()
    except Exception as e:
        print(f"Warning: Could not add common fields: {e}")

    print()
    print("=" * 60)
    print("STEP 4: Fixing hex linkIds in Questionnaire/55")
    print("=" * 60)
    try:
        cmd_fix_hex()
    except Exception as e:
        print(f"Warning: Could not fix hex linkIds: {e}")

    print()
    print("=" * 60)
    print("STEP 5: Fixing duplicate short names")
    print("=" * 60)
    try:
        cmd_fix_duplicates()
    except Exception as e:
        print(f"Warning: Could not fix duplicates: {e}")

    print()
    print("=" * 60)
    print("STEP 6: Finding unmapped UUID fields")
    print("=" * 60)
    try:
        cmd_find_unmapped()
    except Exception as e:
        print(f"Warning: Could not find unmapped fields: {e}")

    print()
    print("=" * 60)
    print("COMPLETE")
    print("=" * 60)


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Manage questionnaire metadata'
    )
    subparsers = parser.add_subparsers(dest='command', help='Command to run')

    subparsers.add_parser('extract-text',
        help='Extract question text from questionnaire JSON files')

    subparsers.add_parser('enrich',
        help='Enrich metadata with labels, short names, and PII flags')

    subparsers.add_parser('add-common',
        help='Add common fields (DOB, Age, etc.) to metadata')

    subparsers.add_parser('fix-hex',
        help='Handle hex-formatted linkIds in Questionnaire/55')

    subparsers.add_parser('fix-duplicates',
        help='Fix duplicate short_name values in metadata')

    subparsers.add_parser('find-unmapped',
        help='Find unmapped UUID fields in database')

    subparsers.add_parser('status',
        help='Show status of metadata files')

    subparsers.add_parser('full-refresh',
        help='Run all operations in sequence')

    args = parser.parse_args()

    if args.command == 'extract-text':
        cmd_extract_text()
    elif args.command == 'enrich':
        cmd_enrich()
    elif args.command == 'add-common':
        cmd_add_common()
    elif args.command == 'fix-hex':
        cmd_fix_hex()
    elif args.command == 'fix-duplicates':
        cmd_fix_duplicates()
    elif args.command == 'find-unmapped':
        cmd_find_unmapped()
    elif args.command == 'status':
        cmd_status()
    elif args.command == 'full-refresh':
        cmd_full_refresh()
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
