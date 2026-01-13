#!/usr/bin/env python3
import csv
import psycopg2
import re
from collections import defaultdict

# Database connection parameters
conn_params = {
    'host': 'localhost',
    'database': 'airbyte',
    'user': 'postgres',
    'password': 'knox48'
}

# Connect to database
conn = psycopg2.connect(**conn_params)
cur = conn.cursor()

# Find all UUID-formatted linkids with their questionnaire context
query = """
SELECT DISTINCT
    a.questionnaire_id,
    a.linkid,
    SUBSTRING(MAX(a.answer_value_text), 1, 100) as sample_value,
    COUNT(DISTINCT a.qr_id) as response_count
FROM airbyte.engage_analytics_engage_analytics_int.int_qr_answers_long a
WHERE a.linkid ~ '^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$'
GROUP BY a.questionnaire_id, a.linkid
ORDER BY a.questionnaire_id, a.linkid
"""

cur.execute(query)
uuid_fields = cur.fetchall()

print(f"Found {len(uuid_fields)} UUID field instances across questionnaires")

# Read existing metadata to check what's already mapped
metadata_file = '/Users/mberg/github/engage-analytics/dbt/seeds/questionnaire_metadata.csv'
existing_metadata = {}
with open(metadata_file, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        key = (row['questionnaire_id'], row['linkid'])
        existing_metadata[key] = row

# Group by linkid to understand patterns
linkid_patterns = defaultdict(list)
for qid, linkid, sample, count in uuid_fields:
    linkid_patterns[linkid].append({
        'questionnaire_id': qid,
        'sample_value': sample,
        'response_count': count
    })

# Find unmapped UUID fields
unmapped = []
for qid, linkid, sample, count in uuid_fields:
    if (qid, linkid) not in existing_metadata:
        unmapped.append({
            'questionnaire_id': qid,
            'linkid': linkid,
            'sample_value': sample,
            'response_count': count
        })

print(f"\nFound {len(unmapped)} unmapped UUID fields")

# Group unmapped by questionnaire for analysis
by_questionnaire = defaultdict(list)
for item in unmapped:
    by_questionnaire[item['questionnaire_id']].append(item)

# Show summary
print("\nUnmapped UUID fields by questionnaire:")
for qid in sorted(by_questionnaire.keys()):
    fields = by_questionnaire[qid]
    print(f"\n{qid} ({len(fields)} fields):")
    for field in fields[:5]:  # Show first 5 as examples
        sample = field['sample_value'] if field['sample_value'] else '[empty]'
        print(f"  - {field['linkid']}: {sample[:50]}")
    if len(fields) > 5:
        print(f"  ... and {len(fields) - 5} more")

# Check if these linkids appear in questionnaire JSON files
print("\n\nChecking questionnaire JSON files for these UUIDs...")

# Map questionnaire IDs to likely file names
qid_to_file = {
    'Questionnaire/56': 'questionnaire/ipc-session-2/sf-pcl-5-ipc-session-2.json',
    'Questionnaire/204': 'questionnaire/ipc-session-1/sf-pcl-5-ipc-session-1.json',
    'Questionnaire/60': 'questionnaire/ipc-session-3/sf-pcl-5-ipc-session-3.json',
    'Questionnaire/210': 'questionnaire/ipc-session-4/sf-pcl-5-ipc-session-4.json',
}

# Save unmapped fields to a CSV for analysis
output_file = '/Users/mberg/github/engage-analytics/dbt/unmapped_uuid_fields.csv'
with open(output_file, 'w', newline='') as f:
    fieldnames = ['questionnaire_id', 'linkid', 'sample_value', 'response_count']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(unmapped)

print(f"\nSaved unmapped fields to {output_file}")

cur.close()
conn.close()