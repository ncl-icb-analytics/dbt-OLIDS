#!/usr/bin/env python3
"""
Schema comparison script - run this in your local environment with dbt/snowflake access
This will help identify discrepancies between sources.yml and actual database schema
"""
import yaml
from pathlib import Path
import subprocess
import json

def load_sources_yml():
    """Load current sources.yml definitions"""
    sources_path = Path('models/sources.yml')
    with open(sources_path, 'r') as f:
        return yaml.safe_load(f)

def get_actual_schema_via_dbt():
    """Get actual schema using dbt run-operation"""
    
    schemas_to_check = [
        ('OLIDS_MASKED', 'Data_Store_OLIDS_UAT'),
        ('OLIDS_TERMINOLOGY', 'Data_Store_OLIDS_UAT'), 
        ('REFERENCE', 'DATA_LAB_OLIDS_UAT')
    ]
    
    actual_schema = {}
    
    for schema_name, database_name in schemas_to_check:
        print(f"Querying {schema_name} schema from {database_name}...")
        
        # Build the SQL query
        sql = f"""
        SELECT 
            table_name,
            column_name,
            data_type
        FROM "{database_name}".INFORMATION_SCHEMA.COLUMNS
        WHERE table_schema = '{schema_name}'
        ORDER BY table_name, ordinal_position
        """
        
        try:
            # Run via dbt
            result = subprocess.run([
                'dbt', 'run-operation', 'run_query', 
                '--args', f'{{"sql": "{sql}"}}'
            ], capture_output=True, text=True, check=True)
            
            # Parse result (this would need to be adapted based on dbt output format)
            # For now, just save the raw output
            with open(f'scripts/actual_{schema_name.lower()}_schema.txt', 'w') as f:
                f.write(result.stdout)
                
        except subprocess.CalledProcessError as e:
            print(f"Error querying {schema_name}: {e}")
            print(f"stderr: {e.stderr}")

def compare_with_sources():
    """Compare actual schema with sources.yml"""
    sources = load_sources_yml()
    
    print("Current sources.yml defines these tables:")
    print("=" * 50)
    
    for source in sources.get('sources', []):
        source_name = source['name']
        schema_name = source['schema']
        print(f"\nSource: {source_name} (Schema: {schema_name})")
        
        for table in source.get('tables', []):
            table_name = table['name']
            print(f"  Table: {table_name}")
            
            columns = table.get('columns', [])
            print(f"    Defined columns ({len(columns)}):")
            for col in columns:
                col_name = col['name']
                col_type = col.get('data_type', 'unknown')
                print(f"      - {col_name} ({col_type})")

def generate_schema_queries():
    """Generate SQL queries you can run manually"""
    queries = {
        'OLIDS_MASKED': """
USE DATABASE "Data_Store_OLIDS_UAT";
SELECT 
    table_name,
    column_name,
    data_type,
    ordinal_position
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'OLIDS_MASKED'
ORDER BY table_name, ordinal_position;
        """,
        
        'OLIDS_TERMINOLOGY': """
USE DATABASE "Data_Store_OLIDS_UAT";
SELECT 
    table_name,
    column_name,
    data_type,
    ordinal_position
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'OLIDS_TERMINOLOGY'
ORDER BY table_name, ordinal_position;
        """,
        
        'REFERENCE': """
USE DATABASE "DATA_LAB_OLIDS_UAT";
SELECT 
    table_name,
    column_name,
    data_type,
    ordinal_position
FROM INFORMATION_SCHEMA.COLUMNS
WHERE table_schema = 'REFERENCE'
ORDER BY table_name, ordinal_position;
        """
    }
    
    # Save queries to files
    for schema_name, query in queries.items():
        with open(f'scripts/query_{schema_name.lower()}_schema.sql', 'w') as f:
            f.write(query)
    
    print("Generated SQL query files:")
    for schema_name in queries.keys():
        print(f"  - scripts/query_{schema_name.lower()}_schema.sql")

if __name__ == "__main__":
    print("Schema Comparison Tool")
    print("=" * 30)
    
    # Show current sources.yml definitions
    compare_with_sources()
    
    # Generate manual query files
    print("\n" + "=" * 50)
    generate_schema_queries()
    
    print("\nNext steps:")
    print("1. Run the generated SQL queries in Snowflake")
    print("2. Compare results with the sources.yml definitions shown above")
    print("3. Update sources.yml and staging models with correct column names")