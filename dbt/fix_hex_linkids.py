#!/usr/bin/env python3
import csv
import psycopg2
import re

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

# Find all linkids that look like UUIDs with dashes (8-4-4-4-12 pattern)
# but aren't standard UUIDs (not all lowercase hex)
query = """
SELECT DISTINCT
    a.questionnaire_id,
    a.linkid,
    MAX(a.answer_value_text) as sample_value
FROM airbyte.engage_analytics_engage_analytics_int.int_qr_answers_long a
WHERE a.linkid ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
AND a.questionnaire_id = 'Questionnaire/55'
GROUP BY a.questionnaire_id, a.linkid
ORDER BY a.linkid
"""

cur.execute(query)
hex_fields = cur.fetchall()

print(f"Found {len(hex_fields)} hex-formatted linkids in Questionnaire/55")

# Read existing metadata
metadata_file = '/Users/mberg/github/engage-analytics/dbt/seeds/questionnaire_metadata.csv'
existing_data = []
existing_keys = set()

with open(metadata_file, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        existing_data.append(row)
        existing_keys.add((row['questionnaire_id'], row['linkid']))

# Create new entries for hex fields
new_entries = []
order_offset = 2000

for qid, linkid, sample in hex_fields:
    # Skip if already exists
    if (qid, linkid) in existing_keys:
        continue
    
    # Create readable alias from sample value
    if sample:
        # Clean up the sample text for alias
        text = sample[:80]  # Limit length
        alias = re.sub(r'[^\w\s]', '', text)
        alias = re.sub(r'\s+', '_', alias)
        alias = alias.lower()[:50]  # Further limit for column names
        alias = f"ipc_s1_{alias}" if alias else f"ipc_s1_field_{linkid[:8]}"
    else:
        text = "Field"
        alias = f"ipc_s1_field_{linkid[:8]}"
    
    new_entry = {
        'questionnaire_id': qid,
        'linkid': linkid,
        'question_text': sample[:100] if sample else 'Field',
        'question_alias': alias,
        'question_order': str(order_offset)
    }
    new_entries.append(new_entry)
    order_offset += 1
    
    print(f"  {linkid}: {alias}")

# Combine all data
all_data = existing_data + new_entries
all_data.sort(key=lambda x: (x['questionnaire_id'], int(x['question_order']) if x['question_order'] else 999))

# Write back to file
with open(metadata_file, 'w', newline='') as f:
    fieldnames = ['questionnaire_id', 'linkid', 'question_text', 'question_alias', 'question_order']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(all_data)

print(f"\nAdded {len(new_entries)} new metadata entries")
print(f"Total metadata entries: {len(all_data)}")

cur.close()
conn.close()