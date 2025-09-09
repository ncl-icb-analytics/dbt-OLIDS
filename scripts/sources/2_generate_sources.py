import pandas as pd
import yaml
import os
import sys

# Path configuration
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(CURRENT_DIR)  # scripts directory
PROJECT_DIR = os.path.dirname(SCRIPTS_DIR)  # actual project root
INPUT_FILE = os.path.join(CURRENT_DIR, 'table_metadata.csv')
OUTPUT_FILE = os.path.join(PROJECT_DIR, 'models', 'sources.yml')
MAPPINGS_FILE = os.path.join(CURRENT_DIR, 'source_mappings.yml')
IGNORE_LIST_FILE = os.path.join(CURRENT_DIR, 'sources_ignore_list.yml')

def load_ignore_list():
    """Load tables to ignore from YAML file"""
    ignored_tables = set()
    
    if os.path.exists(IGNORE_LIST_FILE):
        with open(IGNORE_LIST_FILE, 'r') as f:
            ignore_data = yaml.safe_load(f)
            
        # Handle preserved models - we should ignore their source tables
        if ignore_data and 'preserved_models' in ignore_data:
            for item in ignore_data['preserved_models']:
                if 'source_table' in item:
                    # Assume olids_core schema if not specified differently
                    schema = 'OLIDS_CORE'  # Default schema
                    table = item['source_table']
                    key = (schema.upper(), table.upper())
                    ignored_tables.add(key)
                    print(f"Will ignore source table: {schema}.{table} (preserved model: {item['model']})")
    
    return ignored_tables

def load_source_mappings():
    """Load source mappings from YAML file"""
    if not os.path.exists(MAPPINGS_FILE):
        print(f"Error: Mappings file not found at {MAPPINGS_FILE}", file=sys.stderr)
        sys.exit(1)
        
    with open(MAPPINGS_FILE, 'r') as f:
        mappings_data = yaml.safe_load(f)
    
    # Convert to lookup dictionaries
    db_schema_to_source = {}
    
    for source in mappings_data['sources']:
        db = source['database']  # Preserve original case
        schema = source.get('schema', '')  # Preserve original case
        
        if schema:
            # Specific schema mapping - use uppercase for lookup key only
            key = (db.upper(), schema.upper())
        else:
            # Database-level mapping (all schemas)
            key = (db.upper(), '*')
            
        db_schema_to_source[key] = source
    
    return db_schema_to_source

def find_source_mapping(database, schema, mappings):
    """Find the appropriate source mapping for a database/schema combination"""
    database = database.upper()
    schema = schema.upper()
    
    # First try exact match
    if (database, schema) in mappings:
        return mappings[(database, schema)]
    
    # Then try database-level match
    if (database, '*') in mappings:
        return mappings[(database, '*')]
    
    return None

def main():
    # Load mappings and ignore list
    mappings = load_source_mappings()
    print(f"Loaded {len(mappings)} source mappings from {MAPPINGS_FILE}")
    
    ignored_tables = load_ignore_list()
    if ignored_tables:
        print(f"Loaded {len(ignored_tables)} tables to ignore")
    
    # Try comma first, fallback to tab if error
    try:
        df = pd.read_csv(INPUT_FILE, sep=',')
        df.columns = df.columns.str.strip()
        print("Columns found in file (comma):", df.columns.tolist())
        if len(df.columns) == 1:
            raise ValueError("Only one column found, trying tab delimiter.")
    except Exception:
        df = pd.read_csv(INPUT_FILE, sep='\t')
        df.columns = df.columns.str.strip()
        print("Columns found in file (tab):", df.columns.tolist())

    # Group by database and schema to create sources
    sources = []
    sources_created = {}
    
    for (database, schema), group in df.groupby(['DATABASE_NAME', 'SCHEMA_NAME']):
        # Find mapping for this database/schema
        mapping = find_source_mapping(database, schema, mappings)
        
        if not mapping:
            print(f"Warning: No mapping found for {database}.{schema}, skipping...")
            continue
            
        # Check if source is enabled
        if not mapping.get('enabled', True):  # Default to True if not specified
            print(f"Info: Source '{mapping['source_name']}' is disabled, skipping...")
            continue
        
        source_name = mapping['source_name']
        
        # Track which sources we've created
        if source_name not in sources_created:
            tables = []
            
            # Get all tables for this source
            for table_name, table_group in group.groupby('TABLE_NAME'):
                # Check if table is in ignore list
                if (schema.upper(), table_name.upper()) in ignored_tables:
                    print(f"  Skipping ignored table: {schema}.{table_name}")
                    continue
                
                # Check if specific tables are configured
                if 'tables' in mapping and table_name not in mapping['tables']:
                    continue
                    
                # Sort columns by ordinal position
                sorted_columns = table_group.sort_values('ORDINAL_POSITION')
                table = {
                    'name': table_name,
                    'identifier': f'"{table_name}"',  # Quote identifier to preserve case
                    'columns': [{'name': col, 'data_type': dtype} 
                               for col, dtype in zip(sorted_columns['COLUMN_NAME'], 
                                                   sorted_columns['DATA_TYPE'])]
                }
                tables.append(table)

            source = {
                'name': source_name,
                'database': f'"{mapping["database"]}"',  # Quote for case sensitivity
                'schema': f'"{mapping.get("schema", schema)}"',  # Quote schema and use mapped case
                'description': mapping.get('description', ''),
                'tables': tables
            }
            sources.append(source)
            sources_created[source_name] = True
            
            print(f"Created source '{source_name}' with {len(tables)} tables")

    # Create sources.yml content
    sources_yml = {
        'version': 2,
        'sources': sources
    }

    # Write to file
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        yaml.dump(sources_yml, f, sort_keys=False, default_flow_style=False)
        
    print(f"\nGenerated {OUTPUT_FILE}")
    print(f"Created {len(sources)} data sources with {sum(len(s['tables']) for s in sources)} total tables")
    print(f"\nNext step: Generate staging models:")
    print(f"  python scripts/sources/3_generate_staging_models.py")

if __name__ == '__main__':
    main()