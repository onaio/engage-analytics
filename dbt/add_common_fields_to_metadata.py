#!/usr/bin/env python3
import csv
import psycopg2
from collections import defaultdict

# Database connection parameters
conn_params = {
    'host': 'localhost',
    'database': 'airbyte',
    'user': 'postgres',
    'password': 'knox48'
}

# Common field mappings
common_fields = {
    'cd8e3d6d-e9ff-458d-d122-57070bebffaf': {
        'text': 'Date of Birth',
        'alias': 'date_of_birth',
        'order': 900
    },
    'b5bc7f80-4a0c-486c-e5eb-32c750036f94': {
        'text': 'Age',
        'alias': 'age',
        'order': 901
    },
    'calculated-month': {
        'text': 'Birth Month',
        'alias': 'birth_month',
        'order': 902
    },
    'calculated-year': {
        'text': 'Age in Years',
        'alias': 'age_years',
        'order': 903
    },
    'LINK_ID_THAT_CONTAINS_ENCOUNTER_ID_AS_ANSWER': {
        'text': 'Encounter Reference',
        'alias': 'encounter_reference',
        'order': 904
    }
}

# Get questionnaires that use these fields from database
conn = psycopg2.connect(**conn_params)
cur = conn.cursor()

# Find which questionnaires use which common fields
questionnaire_fields = defaultdict(set)

for linkid in common_fields.keys():
    query = """
    SELECT DISTINCT questionnaire_id
    FROM airbyte.engage_analytics_engage_analytics_int.int_qr_answers_long
    WHERE linkid = %s
    """
    cur.execute(query, (linkid,))
    for row in cur.fetchall():
        questionnaire_fields[row[0]].add(linkid)

cur.close()
conn.close()

# Read existing metadata
metadata_file = '/Users/mberg/github/engage-analytics/dbt/seeds/questionnaire_metadata.csv'
existing_data = []
existing_keys = set()

with open(metadata_file, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        existing_data.append(row)
        # Track existing questionnaire/linkid combinations
        existing_keys.add((row['questionnaire_id'], row['linkid']))

# Add new entries for common fields
new_entries = []
for questionnaire_id, linkids in questionnaire_fields.items():
    for linkid in linkids:
        # Skip if this combination already exists
        if (questionnaire_id, linkid) in existing_keys:
            continue
        
        field_info = common_fields[linkid]
        new_entry = {
            'questionnaire_id': questionnaire_id,
            'linkid': linkid,
            'question_text': field_info['text'],
            'question_alias': field_info['alias'],
            'question_order': str(field_info['order'])
        }
        new_entries.append(new_entry)

# Combine and sort all data
all_data = existing_data + new_entries
all_data.sort(key=lambda x: (x['questionnaire_id'], int(x['question_order']) if x['question_order'] else 999))

# Write back to file
with open(metadata_file, 'w', newline='') as f:
    fieldnames = ['questionnaire_id', 'linkid', 'question_text', 'question_alias', 'question_order']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(all_data)

print(f"Added {len(new_entries)} new metadata entries for common fields")
print(f"Total metadata entries: {len(all_data)}")

# Show summary of changes
print("\nSummary of additions:")
for questionnaire_id in sorted(questionnaire_fields.keys()):
    fields_added = []
    for linkid in questionnaire_fields[questionnaire_id]:
        if (questionnaire_id, linkid) not in existing_keys:
            fields_added.append(common_fields[linkid]['alias'])
    if fields_added:
        print(f"  {questionnaire_id}: {', '.join(fields_added)}")