#!/usr/bin/env python3
import psycopg2
import csv
import os
from pathlib import Path

# Database connection parameters
conn_params = {
    'host': 'localhost',
    'database': 'airbyte',
    'user': 'postgres',
    'password': 'knox48'
}

# Output directory
output_dir = Path('/Users/mberg/github/engage-analytics/dbt/data')
output_dir.mkdir(exist_ok=True)

# Connect to database
conn = psycopg2.connect(**conn_params)
cur = conn.cursor()

# Get all questionnaire mart views
query = """
SELECT table_name
FROM information_schema.views
WHERE table_schema = 'engage_analytics_engage_analytics_mart'
AND table_name LIKE 'qr_%'
ORDER BY table_name
"""

cur.execute(query)
views = [row[0] for row in cur.fetchall()]

print(f"Found {len(views)} questionnaire views to export")

# Export each view to CSV
for view_name in views:
    csv_file = output_dir / f"{view_name}.csv"
    
    try:
        # Get all data from the view
        query = f'SELECT * FROM engage_analytics_engage_analytics_mart."{view_name}"'
        
        # Use COPY command for efficient export
        copy_query = f"""
        COPY (
            SELECT * FROM engage_analytics_engage_analytics_mart."{view_name}"
        ) TO STDOUT WITH CSV HEADER
        """
        
        with open(csv_file, 'w') as f:
            cur.copy_expert(copy_query, f)
        
        # Get row count
        cur.execute(f'SELECT COUNT(*) FROM engage_analytics_engage_analytics_mart."{view_name}"')
        row_count = cur.fetchone()[0]
        
        # Get file size
        file_size = csv_file.stat().st_size
        file_size_mb = file_size / (1024 * 1024)
        
        print(f"✓ {view_name}: {row_count:,} rows, {file_size_mb:.2f} MB")
        
    except Exception as e:
        print(f"✗ Error exporting {view_name}: {e}")

# Also export the other mart views (non-questionnaire)
other_views = ['current_practitioner_role', 'unnested_careteams']
for view_name in other_views:
    csv_file = output_dir / f"{view_name}.csv"
    
    try:
        copy_query = f"""
        COPY (
            SELECT * FROM engage_analytics_engage_analytics_mart."{view_name}"
        ) TO STDOUT WITH CSV HEADER
        """
        
        with open(csv_file, 'w') as f:
            cur.copy_expert(copy_query, f)
        
        cur.execute(f'SELECT COUNT(*) FROM engage_analytics_engage_analytics_mart."{view_name}"')
        row_count = cur.fetchone()[0]
        
        file_size = csv_file.stat().st_size
        file_size_mb = file_size / (1024 * 1024)
        
        print(f"✓ {view_name}: {row_count:,} rows, {file_size_mb:.2f} MB")
        
    except Exception as e:
        print(f"✗ Error exporting {view_name}: {e}")

cur.close()
conn.close()

# Summary
csv_files = list(output_dir.glob('*.csv'))
total_size = sum(f.stat().st_size for f in csv_files) / (1024 * 1024)

print(f"\n{'='*50}")
print(f"Export complete!")
print(f"Total files: {len(csv_files)}")
print(f"Total size: {total_size:.2f} MB")
print(f"Location: {output_dir}")