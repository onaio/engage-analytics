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

# Connect to database to get sample values
conn = psycopg2.connect(**conn_params)
cur = conn.cursor()

# Get sample values for all Questionnaire/55 linkids
query = """
SELECT DISTINCT
    linkid,
    MAX(answer_value_text) as sample_value
FROM airbyte.engage_analytics_engage_analytics_int.int_qr_answers_long
WHERE questionnaire_id = 'Questionnaire/55'
AND linkid ~ '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
GROUP BY linkid
"""

cur.execute(query)
linkid_samples = {row[0]: row[1] for row in cur.fetchall()}
cur.close()
conn.close()

print(f"Found {len(linkid_samples)} hex linkids with sample values")

# Read existing metadata
metadata_file = '/Users/mberg/github/engage-analytics/dbt/seeds/questionnaire_metadata.csv'
all_data = []

with open(metadata_file, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        # Check if this is a Questionnaire/55 entry with a hex linkid
        if (row['questionnaire_id'] == 'Questionnaire/55' and 
            row['linkid'] in linkid_samples):
            
            sample = linkid_samples[row['linkid']]
            
            # Update if the current alias is just the hex string without dashes
            if re.match(r'^ipc_s1_[0-9a-f]{32}$', row['question_alias']):
                if sample:
                    # Create a better alias from the sample value
                    text = sample[:60]
                    alias = re.sub(r'[^\w\s]', '', text)
                    alias = re.sub(r'\s+', '_', alias)
                    alias = alias.lower()
                    # Truncate to reasonable length
                    if len(alias) > 40:
                        alias = alias[:40]
                    alias = f"ipc_s1_{alias}"
                    
                    row['question_text'] = sample[:100]
                    row['question_alias'] = alias[:63]  # PostgreSQL limit
                    print(f"Updated {row['linkid'][:8]}... to: {alias[:63]}")
                else:
                    # No sample value, create a shorter default alias
                    row['question_alias'] = f"ipc_s1_field_{row['linkid'][:8]}"
        
        all_data.append(row)

# Write back to file
with open(metadata_file, 'w', newline='') as f:
    fieldnames = ['questionnaire_id', 'linkid', 'question_text', 'question_alias', 'question_order']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(all_data)

print(f"\nUpdated metadata file with {len(all_data)} total entries")