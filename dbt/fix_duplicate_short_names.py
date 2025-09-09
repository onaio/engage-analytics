#!/usr/bin/env python3
"""
Fix duplicate short_name values in questionnaire_metadata.csv by adding numeric suffixes
"""
import csv
from collections import defaultdict

def fix_duplicate_short_names():
    metadata_path = '/Users/mberg/github/engage-analytics/dbt/seeds/questionnaire_metadata.csv'
    
    # Read existing metadata
    with open(metadata_path, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    print(f"Read {len(rows)} metadata rows")
    
    # Track short names per table
    table_short_names = defaultdict(lambda: defaultdict(list))
    
    # First pass: collect all short names per table
    for i, row in enumerate(rows):
        table = row['table']
        short_name = row['short_name']
        table_short_names[table][short_name].append(i)
    
    # Count duplicates
    duplicate_count = 0
    for table, short_names in table_short_names.items():
        for short_name, indices in short_names.items():
            if len(indices) > 1:
                duplicate_count += len(indices) - 1
                print(f"Table {table}: '{short_name}' appears {len(indices)} times")
    
    print(f"\nFound {duplicate_count} duplicate short names across all tables")
    
    # Second pass: fix duplicates by adding numeric suffixes
    for table, short_names in table_short_names.items():
        for short_name, indices in short_names.items():
            if len(indices) > 1:
                # Keep the first one as-is, add suffixes to the rest
                for j, idx in enumerate(indices[1:], start=2):
                    new_short_name = f"{short_name}_{j}"
                    # Ensure it doesn't exceed 30 characters
                    if len(new_short_name) > 30:
                        # Truncate the base name to make room for suffix
                        base = short_name[:30-len(f"_{j}")]
                        new_short_name = f"{base}_{j}"
                    rows[idx]['short_name'] = new_short_name
    
    # Write back the fixed metadata
    fieldnames = ['table', 'column', 'short_name', 'label', 'data_type', 'questionnaire_title', 'source', 'anon']
    
    with open(metadata_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"\nFixed {duplicate_count} duplicate short names")
    print(f"Updated {metadata_path}")
    
    # Verify no duplicates remain
    table_short_names_after = defaultdict(lambda: defaultdict(list))
    for i, row in enumerate(rows):
        table = row['table']
        short_name = row['short_name']
        table_short_names_after[table][short_name].append(i)
    
    remaining_duplicates = 0
    for table, short_names in table_short_names_after.items():
        for short_name, indices in short_names.items():
            if len(indices) > 1:
                remaining_duplicates += 1
                print(f"WARNING: Still duplicate in {table}: {short_name}")
    
    if remaining_duplicates == 0:
        print("✓ All duplicates successfully resolved")
    else:
        print(f"✗ {remaining_duplicates} duplicates still remain")

if __name__ == '__main__':
    fix_duplicate_short_names()