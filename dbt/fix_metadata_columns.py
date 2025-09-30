#!/usr/bin/env python3
"""
Fix metadata by swapping column and short_name values
- column should be the friendly name (what shows in database)
- linkid should be the UUID (for reference)
"""
import csv

# Read current metadata
metadata_file = '/Users/mberg/github/engage-analytics/dbt/seeds/questionnaire_metadata.csv'
rows = []

with open(metadata_file, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        # For questionnaire source rows, add linkid and swap column/short_name
        if row['source'] == 'questionnaire':
            # Store the UUID as linkid
            linkid = row['column']
            # Use short_name as the column name
            column = row['short_name']
            # Keep short_name the same
            short_name = row['short_name']

            row['linkid'] = linkid
            row['column'] = column
            row['short_name'] = short_name
        else:
            # For system and generated fields, no linkid needed
            row['linkid'] = ''

        rows.append(row)

# Write updated metadata with linkid column
with open(metadata_file, 'w', newline='') as f:
    fieldnames = ['table', 'column', 'linkid', 'short_name', 'label', 'data_type', 'questionnaire_title', 'source', 'anon']
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

print(f"Updated {len(rows)} metadata rows")
print(f"Added linkid column for questionnaire fields")

# Show examples
print("\nExamples:")
questionnaire_rows = [r for r in rows if r['source'] == 'questionnaire'][:5]
for r in questionnaire_rows:
    print(f"  Table: {r['table']}")
    print(f"    Column: {r['column']}")
    print(f"    LinkID: {r['linkid'][:40]}...")
    print(f"    Label: {r['label'][:60]}...")
    print()
