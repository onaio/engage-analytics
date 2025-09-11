#!/usr/bin/env python3
"""
Test the export functionality directly
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pathlib import Path
from datetime import datetime
import zipfile
from export_handler import DataExporter

def test_export():
    """Test the export process"""
    print("Testing Export Process")
    print("-" * 50)
    
    # Create export directory
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    exports_base = Path('/Users/mberg/github/engage-analytics/dataexport/exports')
    exports_base.mkdir(exist_ok=True)
    export_dir = exports_base / f"test_export_{timestamp}"
    export_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"Export directory: {export_dir}")
    
    try:
        # Create exporter
        exporter = DataExporter()
        print("Connecting to database...")
        exporter.connect()
        
        # Get tables
        print("Fetching tables...")
        tables = exporter.get_anonymized_tables()
        print(f"Found {len(tables)} tables")
        
        # Export first 3 tables as a test
        test_tables = tables[:3]
        print(f"\nExporting {len(test_tables)} tables as test:")
        
        for table in test_tables:
            csv_file = export_dir / f"{table}.csv"
            print(f"  Exporting {table}...")
            result = exporter.export_table_to_csv(table, csv_file)
            print(f"    → {result['rows']} rows, {result['size']} bytes")
        
        # Create zip
        print("\nCreating ZIP file...")
        zip_path = export_dir / f"test_export_{timestamp}.zip"
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            csv_files = list(export_dir.glob('*.csv'))
            print(f"Adding {len(csv_files)} CSV files to ZIP...")
            for csv_file in csv_files:
                zipf.write(csv_file, csv_file.name)
                print(f"  Added: {csv_file.name}")
        
        print(f"\nZIP created: {zip_path}")
        print(f"ZIP size: {zip_path.stat().st_size / (1024*1024):.2f} MB")
        
        # List contents of ZIP
        print("\nZIP contents:")
        with zipfile.ZipFile(zip_path, 'r') as zipf:
            for info in zipf.filelist:
                print(f"  {info.filename}: {info.file_size} bytes (compressed: {info.compress_size} bytes)")
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        exporter.disconnect()
        print("\nTest complete!")
        print(f"Check the export at: {export_dir}")

if __name__ == "__main__":
    test_export()