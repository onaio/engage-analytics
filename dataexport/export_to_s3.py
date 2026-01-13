#!/usr/bin/env python3
# ABOUTME: CLI script to export engage analytics data to S3.
# ABOUTME: Supports both anonymized and PII exports to separate buckets.

import argparse
import os
import shutil
import zipfile
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from export_handler import DataExporter

load_dotenv()


def create_zip(source_dir, zip_path):
    """Create a ZIP file from a directory of CSVs"""
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for file in Path(source_dir).glob('*.csv'):
            zipf.write(file, file.name)
    return zip_path


def run_export(export_type, delete_local=False):
    """Run export for a single type (anon or pii) and upload to S3"""
    bucket_env = 'S3_BUCKET_ANON' if export_type == 'anon' else 'S3_BUCKET_PII'
    bucket = os.getenv(bucket_env)

    if not bucket:
        raise ValueError(f"Missing environment variable: {bucket_env}")

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    export_dir = Path(f'exports/export_{timestamp}_{export_type}')
    zip_filename = f'engage_analytics_export_{export_type}_{timestamp}.zip'
    zip_path = export_dir.parent / zip_filename

    exporter = DataExporter()

    try:
        print(f"\n{'='*60}")
        print(f"Starting {export_type.upper()} export")
        print(f"{'='*60}")

        print("Connecting to database...")
        exporter.connect()

        print(f"Exporting tables to {export_dir}...")
        results = exporter.export_all_to_directory(export_dir, export_type)

        total_rows = sum(r['rows'] for r in results)
        print(f"Exported {len(results)} tables, {total_rows:,} total rows")

        print(f"Creating ZIP: {zip_path}")
        create_zip(export_dir, zip_path)
        zip_size = zip_path.stat().st_size / (1024 * 1024)
        print(f"ZIP size: {zip_size:.2f} MB")

        now = datetime.now()
        s3_key = f"{now.year}/{now.month:02d}/{now.day:02d}/{zip_filename}"

        print(f"Uploading to s3://{bucket}/{s3_key}")
        exporter.upload_to_s3(zip_path, bucket, s3_key)

        print(f"Upload complete!")

        if delete_local:
            print("Cleaning up local files...")
            shutil.rmtree(export_dir)
            zip_path.unlink()
            print("Local files removed")
        else:
            print(f"Local files preserved at {export_dir} and {zip_path}")

        return {
            'type': export_type,
            'tables': len(results),
            'rows': total_rows,
            's3_uri': f"s3://{bucket}/{s3_key}"
        }

    finally:
        exporter.disconnect()


def main():
    parser = argparse.ArgumentParser(
        description='Export engage analytics data to S3'
    )
    parser.add_argument(
        '--type',
        choices=['anon', 'pii', 'both'],
        default='both',
        help='Export type: anon, pii, or both (default: both)'
    )
    parser.add_argument(
        '--delete-local',
        action='store_true',
        help='Delete local files after upload (default: keep)'
    )

    args = parser.parse_args()

    exports_dir = Path('exports')
    exports_dir.mkdir(exist_ok=True)

    results = []

    if args.type in ('anon', 'both'):
        result = run_export('anon', args.delete_local)
        results.append(result)

    if args.type in ('pii', 'both'):
        result = run_export('pii', args.delete_local)
        results.append(result)

    print(f"\n{'='*60}")
    print("EXPORT SUMMARY")
    print(f"{'='*60}")
    for r in results:
        print(f"  {r['type'].upper()}: {r['tables']} tables, {r['rows']:,} rows")
        print(f"    -> {r['s3_uri']}")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
