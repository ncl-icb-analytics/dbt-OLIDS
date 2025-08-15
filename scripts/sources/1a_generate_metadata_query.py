#!/usr/bin/env python3
"""
Generate dynamic SQL query to extract metadata from all databases/schemas 
defined in source_mappings.yml

This replaces the static SQL file with a dynamic query based on your 
source configuration.

Usage:
    python 1_generate_metadata_query.py
    # Then run the generated metadata_query.sql in Snowflake UI
"""

import yaml
import os
import sys

# Path configuration
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

def load_source_mappings():
    """Load source mappings from YAML file"""
    mappings_file = os.path.join(CURRENT_DIR, 'source_mappings.yml')
    
    if not os.path.exists(mappings_file):
        print(f"Error: source_mappings.yml not found at {mappings_file}", file=sys.stderr)
        sys.exit(1)
        
    with open(mappings_file, 'r') as f:
        return yaml.safe_load(f)

def generate_sql_query(mappings_data):
    """Generate dynamic SQL query from source mappings"""
    
    # Start building the query
    query_parts = []
    query_parts.append("-- Dynamically generated metadata query from source_mappings.yml")
    query_parts.append("-- Query all databases and schemas defined in your source configuration")
    query_parts.append("--")
    query_parts.append("-- Usage:")
    query_parts.append("--   1. Copy this entire query")
    query_parts.append("--   2. Paste into Snowflake UI") 
    query_parts.append("--   3. Execute query")
    query_parts.append("--   4. Export results as CSV to table_metadata.csv")
    query_parts.append("")
    query_parts.append("WITH schema_metadata AS (")
    
    union_parts = []
    
    for i, source in enumerate(mappings_data['sources']):
        database = source['database']
        schema = source.get('schema', '')
        source_name = source['source_name']
        description = source.get('description', '')
        
        if not schema:
            # Skip database-level mappings for now - would need to query all schemas
            continue
            
        # Add comment for this source
        comment = f"  -- {source_name}: {description}" if description else f"  -- {source_name}"
        
        # Generate the SELECT statement for this database/schema
        select_part = f"""  {comment}
  SELECT 
    '{database}' as database_name,
    '{schema}' as schema_name,
    table_name,
    column_name,
    data_type,
    ordinal_position
  FROM "{database}".INFORMATION_SCHEMA.COLUMNS
  WHERE table_schema = '{schema}'"""
        
        union_parts.append(select_part)
    
    # Join all parts with UNION ALL
    query_parts.append("\n  \n  UNION ALL\n  \n".join(union_parts))
    query_parts.append(")")
    query_parts.append("")
    query_parts.append("SELECT ")
    query_parts.append('  database_name as "DATABASE_NAME",')
    query_parts.append('  schema_name as "SCHEMA_NAME", ')
    query_parts.append('  table_name as "TABLE_NAME",')
    query_parts.append('  column_name as "COLUMN_NAME",')
    query_parts.append('  data_type as "DATA_TYPE",')
    query_parts.append('  ordinal_position as "ORDINAL_POSITION"')
    query_parts.append("FROM schema_metadata")
    query_parts.append("ORDER BY database_name, schema_name, table_name, ordinal_position;")
    
    return "\n".join(query_parts)

def main():
    """Main function"""
    mappings_data = load_source_mappings()
    
    if 'sources' not in mappings_data:
        print("Error: No 'sources' section found in source_mappings.yml", file=sys.stderr)
        sys.exit(1)
    
    sql_query = generate_sql_query(mappings_data)
    
    # Write to file
    output_file = os.path.join(CURRENT_DIR, 'metadata_query.sql')
    
    with open(output_file, 'w') as f:
        f.write(sql_query)
    
    print(f"Query saved to {output_file}")
    print(f"\nNext step: Run script 1b to extract metadata automatically:")
    print(f"  python scripts/sources/1b_extract_metadata.py")

if __name__ == '__main__':
    main()