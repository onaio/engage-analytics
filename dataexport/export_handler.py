#!/usr/bin/env python3
"""
Data export handler for PostgreSQL database
Exports anonymized views to CSV format
"""

import os
import csv
import psycopg2
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
    
    def export_table_to_csv(self, table_name, output_file):
        """Export a single table/view to CSV file"""
        try:
            # Use COPY command for efficient export
            copy_query = f"""
            COPY (
                SELECT * FROM {self.schema}."{table_name}"
            ) TO STDOUT WITH CSV HEADER
            """
            
            with open(output_file, 'w') as f:
                self.cur.copy_expert(copy_query, f)
            
            # Get row count for logging
            self.cur.execute(f'SELECT COUNT(*) FROM {self.schema}."{table_name}"')
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
    
    def export_all_to_directory(self, output_dir):
        """Export all anonymized tables to a directory"""
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        results = []
        tables = self.get_anonymized_tables()
        
        for table in tables:
            csv_file = output_path / f"{table}.csv"
            result = self.export_table_to_csv(table, csv_file)
            results.append(result)
            print(f"Exported {table}: {result['rows']} rows")
        
        return results


# Command-line interface for testing
if __name__ == '__main__':
    import sys
    
    print("Data Exporter Test")
    print("-" * 50)
    
    exporter = DataExporter()
    
    try:
        print("Connecting to database...")
        exporter.connect()
        print("Connected successfully!")
        
        print("\nFetching anonymized tables...")
        tables = exporter.get_anonymized_tables()
        print(f"Found {len(tables)} tables to export:")
        for table in tables[:10]:  # Show first 10
            print(f"  - {table}")
        if len(tables) > 10:
            print(f"  ... and {len(tables) - 10} more")
        
        # Export to test directory if requested
        if len(sys.argv) > 1 and sys.argv[1] == '--export':
            output_dir = Path('test_export')
            print(f"\nExporting to {output_dir}...")
            results = exporter.export_all_to_directory(output_dir)
            
            total_size = sum(r['size'] for r in results) / (1024 * 1024)
            total_rows = sum(r['rows'] for r in results)
            
            print(f"\nExport complete!")
            print(f"Total files: {len(results)}")
            print(f"Total rows: {total_rows:,}")
            print(f"Total size: {total_size:.2f} MB")
    
    except Exception as e:
        print(f"Error: {e}")
    
    finally:
        exporter.disconnect()
        print("\nDisconnected from database")