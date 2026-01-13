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
    print("STEP 2: Finding unmapped UUID fields")
    print("=" * 60)
    try:
        cmd_find_unmapped()
    except Exception as e:
        print(f"Warning: Could not find unmapped fields (database may not be available): {e}")

    print()
    print("=" * 60)
    print("STEP 3: Fixing duplicate short names")
    print("=" * 60)
    try:
        cmd_fix_duplicates()
    except Exception as e:
        print(f"Warning: Could not fix duplicates: {e}")

    print()
    print("=" * 60)
    print("COMPLETE")
    print("=" * 60)
    print("\nNote: For full metadata enrichment, run the original")
    print("update_metadata_with_questions.py script from bak/")


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

    subparsers.add_parser('find-unmapped',
        help='Find unmapped UUID fields in database')

    subparsers.add_parser('fix-duplicates',
        help='Fix duplicate short_name values in metadata')

    subparsers.add_parser('status',
        help='Show status of metadata files')

    subparsers.add_parser('full-refresh',
        help='Run all operations in sequence')

    args = parser.parse_args()

    if args.command == 'extract-text':
        cmd_extract_text()
    elif args.command == 'find-unmapped':
        cmd_find_unmapped()
    elif args.command == 'fix-duplicates':
        cmd_fix_duplicates()
    elif args.command == 'status':
        cmd_status()
    elif args.command == 'full-refresh':
        cmd_full_refresh()
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
