#!/usr/bin/env python3
# ABOUTME: Data export handler for PostgreSQL database.
# ABOUTME: Exports anonymized and PII views to CSV format with S3 upload support.

import os
import psycopg2
import boto3
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class DataExporter:
    """Handle database connections and data exports"""
    
    def __init__(self):
        """Initialize exporter with database configuration"""
        self.conn_params = {
            'host': os.getenv('DBT_HOST', 'localhost'),
            'database': os.getenv('DBT_DATABASE', 'airbyte'),
            'user': os.getenv('DBT_USER', 'postgres'),
            'password': os.getenv('DBT_PASSWORD', 'knox48'),
            'port': os.getenv('DBT_PORT', '5432')
        }
        self.conn = None
        self.cur = None
        self.schema = 'engage_analytics_engage_analytics_mart'
    
    def connect(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(**self.conn_params)
            self.cur = self.conn.cursor()
            return True
        except Exception as e:
            raise Exception(f"Failed to connect to database: {str(e)}")
    
    def disconnect(self):
        """Close database connection"""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
    
    def get_anonymized_tables(self):
        """Get list of anonymized tables/views to export"""
        try:
            # Query for anonymized questionnaire views
            query = f"""
            SELECT table_name
            FROM information_schema.views
            WHERE table_schema = '{self.schema}'
            AND (
                table_name LIKE 'qr_%_anon'
                OR table_name = 'patient_anon'
            )
            ORDER BY table_name
            """

            self.cur.execute(query)
            views = [row[0] for row in self.cur.fetchall()]

            # Also include other anonymized resource tables if they exist
            resource_tables = [
                'encounters',
                'practitioners',
                'organizations',
                'current_practitioner_role',
                'unnested_careteams'
            ]

            for table in resource_tables:
                # Check if table exists
                check_query = f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = '{self.schema}'
                    AND table_name = '{table}'
                )
                """
                self.cur.execute(check_query)
                if self.cur.fetchone()[0]:
                    views.append(table)

            return views

        except Exception as e:
            raise Exception(f"Failed to get table list: {str(e)}")

    def get_metrics_tables(self):
        """Get list of metrics tables to export"""
        try:
            metrics_schema = 'engage_analytics'
            query = f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{metrics_schema}'
            AND table_name LIKE 'fct_metrics%'
            ORDER BY table_name
            """
            self.cur.execute(query)
            return [row[0] for row in self.cur.fetchall()]
        except Exception as e:
            raise Exception(f"Failed to get metrics table list: {str(e)}")

    def get_pii_tables(self):
        """Get list of PII tables/views to export (non-anonymized)"""
        try:
            # Query for non-anonymized questionnaire views
            query = f"""
            SELECT table_name
            FROM information_schema.views
            WHERE table_schema = '{self.schema}'
            AND table_name LIKE 'qr_%'
            AND table_name NOT LIKE 'qr_%_anon'
            ORDER BY table_name
            """

            self.cur.execute(query)
            views = [row[0] for row in self.cur.fetchall()]

            # Also check for patient table (non-anonymized)
            patient_check = f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = '{self.schema}'
                AND table_name = 'patient'
            )
            """
            self.cur.execute(patient_check)
            if self.cur.fetchone()[0]:
                views.insert(0, 'patient')

            # Include other resource tables
            resource_tables = [
                'encounters',
                'practitioners',
                'organizations',
                'current_practitioner_role',
                'unnested_careteams'
            ]

            for table in resource_tables:
                # Check if table exists
                check_query = f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = '{self.schema}'
                    AND table_name = '{table}'
                )
                """
                self.cur.execute(check_query)
                if self.cur.fetchone()[0]:
                    views.append(table)

            return views

        except Exception as e:
            raise Exception(f"Failed to get PII table list: {str(e)}")
    
    def export_table_to_csv(self, table_name, output_file, schema=None):
        """Export a single table/view to CSV file"""
        try:
            export_schema = schema or self.schema
            # Use COPY command for efficient export
            copy_query = f"""
            COPY (
                SELECT * FROM {export_schema}."{table_name}"
            ) TO STDOUT WITH CSV HEADER
            """

            with open(output_file, 'w') as f:
                self.cur.copy_expert(copy_query, f)

            # Get row count for logging
            self.cur.execute(f'SELECT COUNT(*) FROM {export_schema}."{table_name}"')
            row_count = self.cur.fetchone()[0]

            return {
                'table': table_name,
                'rows': row_count,
                'file': str(output_file),
                'size': output_file.stat().st_size if output_file.exists() else 0
            }

        except Exception as e:
            raise Exception(f"Failed to export {table_name}: {str(e)}")
    
    def get_table_info(self, table_name):
        """Get information about a table (columns, row count)"""
        try:
            # Get column information
            col_query = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = '{self.schema}'
            AND table_name = '{table_name}'
            ORDER BY ordinal_position
            """
            self.cur.execute(col_query)
            columns = self.cur.fetchall()
            
            # Get row count
            self.cur.execute(f'SELECT COUNT(*) FROM {self.schema}."{table_name}"')
            row_count = self.cur.fetchone()[0]
            
            return {
                'table': table_name,
                'columns': columns,
                'row_count': row_count
            }
            
        except Exception as e:
            raise Exception(f"Failed to get info for {table_name}: {str(e)}")
    
    def export_all_to_directory(self, output_dir, export_type='anon'):
        """Export tables to a directory based on export type"""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True, parents=True)

        results = []
        schema = None

        if export_type == 'anon':
            tables = self.get_anonymized_tables()
        elif export_type == 'metrics':
            tables = self.get_metrics_tables()
            schema = 'engage_analytics'
        else:
            tables = self.get_pii_tables()

        for table in tables:
            csv_file = output_path / f"{table}.csv"
            result = self.export_table_to_csv(table, csv_file, schema=schema)
            results.append(result)
            print(f"Exported {table}: {result['rows']} rows")

        return results

    def upload_to_s3(self, local_path, bucket, s3_key):
        """Upload a file to S3"""
        s3 = boto3.client(
            's3',
            region_name=os.getenv('AWS_REGION', 'us-east-1')
        )
        s3.upload_file(str(local_path), bucket, s3_key)
        print(f"Uploaded {local_path} to s3://{bucket}/{s3_key}")


if __name__ == '__main__':
    print("Use export_to_s3.py for CLI functionality")