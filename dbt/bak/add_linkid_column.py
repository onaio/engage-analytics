#!/usr/bin/env python3
"""
Add linkid column to questionnaire_metadata.csv by querying current database
"""
import csv
import os
import re
import psycopg2

def get_questionnaire_id_for_table(table_name):
    """Extract questionnaire ID from the table's model file"""
    model_file = f'/Users/mberg/github/engage-analytics/dbt/models/marts/qr_named/{table_name}.sql'
    try:
        with open(model_file, 'r') as f:
            content = f.read()
            match = re.search(r'identifiers\s*=\s*\["(Questionnaire/[^"]+)"\]', content)
            if match:
                return match.group(1)
    except FileNotFoundError:
        pass
    return None

# Connect to database
conn = psycopg2.connect(
    host=os.environ['DBT_HOST'],
    port=os.environ.get('DBT_PORT', 5432),
    database=os.environ['DBT_DATABASE'],
    user=os.environ['DBT_USER'],
    password=os.environ['DBT_PASSWORD']
)
cur = conn.cursor()

print("Connected to database")

# Read current metadata (without linkid)
metadata_file = '/Users/mberg/github/engage-analytics/dbt/seeds/questionnaire_metadata.csv'
current_metadata = []

with open(metadata_file, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        current_metadata.append(row)

print(f"Read {len(current_metadata)} rows from current metadata")

# Get all qr_ tables
cur.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'engage_analytics_engage_analytics_mart'
    AND table_name LIKE 'qr_%'
    AND table_name NOT LIKE '%_anon'
    ORDER BY table_name
""")
tables = [row[0] for row in cur.fetchall()]

print(f"Found {len(tables)} qr_ tables in database")

# For each table, get current columns and map to metadata
table_column_to_linkid = {}

for table_name in tables:
    questionnaire_id = get_questionnaire_id_for_table(table_name)
    if not questionnaire_id:
        continue

    # Get current database columns by position
    cur.execute("""
        SELECT ordinal_position, column_name
        FROM information_schema.columns
        WHERE table_schema = 'engage_analytics_engage_analytics_mart'
        AND table_name = %s
        ORDER BY ordinal_position
    """, (table_name,))

    current_columns_by_position = {row[0]: row[1] for row in cur.fetchall()}

    # Get metadata entries for this table in order
    metadata_entries = [r for r in current_metadata if r['table'] == table_name]

    if table_name not in table_column_to_linkid:
        table_column_to_linkid[table_name] = {}

    # Match by ordinal position
    for idx, metadata_entry in enumerate(metadata_entries):
        ordinal_position = idx + 1
        metadata_col_name = metadata_entry['column']

        if ordinal_position in current_columns_by_position:
            current_col_name = current_columns_by_position[ordinal_position]

            # If names differ, the current one is likely the linkid
            if current_col_name != metadata_col_name:
                table_column_to_linkid[table_name][metadata_col_name] = current_col_name
            elif '-' in current_col_name:
                # Hyphenated names are linkids
                table_column_to_linkid[table_name][metadata_col_name] = current_col_name

    print(f"  {table_name}: {len(table_column_to_linkid.get(table_name, {}))} linkids mapped")

# Build new metadata with linkid column
new_metadata = []

for row in current_metadata:
    table = row['table']
    column = row['column']
    source = row['source']

    new_row = dict(row)

    # Add linkid
    linkid = ''
    if source == 'questionnaire':
        if table in table_column_to_linkid and column in table_column_to_linkid[table]:
            linkid = table_column_to_linkid[table][column]

    new_row['linkid'] = linkid
    new_metadata.append(new_row)

linkid_count = sum(1 for r in new_metadata if r['linkid'])
print(f"\nTotal linkids populated: {linkid_count}")

# Write new metadata with linkid column
with open(metadata_file, 'w', newline='') as f:
    fieldnames = ['table', 'column', 'linkid', 'short_name', 'label', 'data_type', 'questionnaire_title', 'source', 'anon']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(new_metadata)

print(f"Wrote updated metadata to {metadata_file}")

# Show examples
print("\nExample linkids:")
examples = [r for r in new_metadata if r['linkid'] and r['source'] == 'questionnaire'][:10]
for ex in examples:
    print(f"  {ex['table']}: {ex['column']:30} -> {ex['linkid'][:40]}")

cur.close()
conn.close()
