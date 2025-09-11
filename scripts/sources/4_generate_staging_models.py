import yaml
import os
import sys
import re
import argparse

# Path configuration
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(CURRENT_DIR)  # scripts directory
PROJECT_DIR = os.path.dirname(SCRIPTS_DIR)  # actual project root
SOURCES_YML = os.path.join(PROJECT_DIR, 'models', 'sources.yml')
# Note: Staging directory will be determined based on source mapping
MAPPINGS_FILE = os.path.join(CURRENT_DIR, 'source_mappings.yml')
MODEL_TESTS_FILE = os.path.join(CURRENT_DIR, 'default_model_tests.yml')
PRESERVED_MODELS_FILE = os.path.join(CURRENT_DIR, 'sources_ignore_list.yml')

def load_source_mappings():
    """Load source mappings from YAML file"""
    if not os.path.exists(MAPPINGS_FILE):
        print(f"Warning: Mappings file not found at {MAPPINGS_FILE}", file=sys.stderr)
        return {}
        
    with open(MAPPINGS_FILE, 'r') as f:
        mappings_data = yaml.safe_load(f)
    
    # Create lookup by source_name
    mappings_by_name = {}
    for source in mappings_data['sources']:
        mappings_by_name[source['source_name']] = source
    
    return mappings_by_name

def load_preserved_models():
    """Load list of models to preserve from YAML file"""
    preserved = set()
    preserved_tables = {}  # Map source table to preserved model name
    
    if os.path.exists(PRESERVED_MODELS_FILE):
        with open(PRESERVED_MODELS_FILE, 'r') as f:
            data = yaml.safe_load(f)
            
        if data and 'preserved_models' in data:
            for item in data['preserved_models']:
                model_name = item['model']
                preserved.add(model_name)
                if 'source_table' in item:
                    preserved_tables[item['source_table'].upper()] = model_name
                print(f"Will preserve: {model_name} - {item.get('reason', 'No reason provided')}")
    
    return preserved, preserved_tables

def load_model_tests():
    """Load default model tests configuration from YAML file"""
    if not os.path.exists(MODEL_TESTS_FILE):
        print(f"Warning: Model tests file not found at {MODEL_TESTS_FILE}, using hardcoded defaults")
        return {'default_tests': ['all_source_columns_in_staging'], 'model_specific_tests': {}}
        
    with open(MODEL_TESTS_FILE, 'r') as f:
        return yaml.safe_load(f)

def sanitise_filename(name):
    """Convert table name to safe filename by replacing special characters"""
    # Replace problematic characters with underscores
    safe_name = re.sub(r'[&\-\.\s]+', '_', name)
    # Remove multiple consecutive underscores
    safe_name = re.sub(r'_+', '_', safe_name)
    # Remove leading/trailing underscores
    safe_name = safe_name.strip('_')
    return safe_name.lower()

def sanitise_column_name(col_name):
    """Convert column name to SQL-safe identifier"""
    # Handle specific problematic patterns first
    if col_name.lower() in ['nhs_number', 'nhs number']:
        return 'nhs_number_value'
    
    # Replace dots, slashes, hyphens, spaces, and other problematic characters
    safe_name = re.sub(r'[\.\/\&\-\s\(\)\[\]]+', '_', col_name)
    # Remove multiple consecutive underscores
    safe_name = re.sub(r'_+', '_', safe_name)
    # Remove leading/trailing underscores
    safe_name = safe_name.strip('_')
    
    # Handle reserved words and ensure valid SQL identifier
    if safe_name.lower() in ['pseudo', 'group', 'order', 'having', 'where']:
        safe_name = f'{safe_name}_value'
    
    # Convert to lowercase for consistency
    return safe_name.lower()

def generate_schema_files(models_by_domain):
    """Generate schema.yml files for staging models with configured tests."""
    # Load test configuration
    test_config = load_model_tests()
    default_tests = test_config.get('default_tests', ['all_source_columns_in_staging'])
    model_specific_tests = test_config.get('model_specific_tests', {}) or {}
    
    for domain, count in models_by_domain.items():
        if count == 0:
            continue
            
        # Set staging directory based on domain
        if domain == 'shared':
            staging_dir = os.path.join(PROJECT_DIR, 'models', 'shared', 'staging')
        else:
            staging_dir = os.path.join(PROJECT_DIR, 'models', domain, 'staging')
        
        schema_file = os.path.join(staging_dir, 'schema.yml')
        
        # Get all staging model files
        staging_models = []
        for sql_file in os.listdir(staging_dir):
            if sql_file.endswith('.sql') and sql_file.startswith('stg_'):
                model_name = sql_file[:-4]  # Remove .sql extension
                staging_models.append(model_name)
        
        if not staging_models:
            continue
            
        # Generate schema content
        schema_content = {
            'version': 2,
            'models': []
        }
        
        for model_name in sorted(staging_models):
            description = f"Staging model for {model_name.replace('stg_', '').replace('_', ' ')}"
            
            # Start with default tests
            model_tests = default_tests.copy()
            
            # Add model-specific tests if configured
            if model_name in model_specific_tests:
                model_tests.extend(model_specific_tests[model_name])
            
            model_entry = {
                'name': model_name,
                'description': description,
                'tests': model_tests
            }
            schema_content['models'].append(model_entry)
        
        # Write schema file using YAML library for proper formatting
        with open(schema_file, 'w') as f:
            yaml.dump(schema_content, f, default_flow_style=False, sort_keys=False, indent=2)
        
        print(f"  Generated {domain}/staging/schema.yml with {len(staging_models)} models")

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Generate dbt staging models from sources.yml')
    parser.add_argument('--auto-schema', action='store_true',
                       help='Automatically generate schema.yml files with default tests after creating staging models')
    
    args = parser.parse_args()
    
    # Load source mappings
    mappings = load_source_mappings()
    
    # Load preserved models
    preserved_models, preserved_tables = load_preserved_models()
    if preserved_models:
        print(f"Loaded {len(preserved_models)} models to preserve")
    
    # Load sources.yml
    if not os.path.exists(SOURCES_YML):
        print(f"Error: sources.yml not found at {SOURCES_YML}", file=sys.stderr)
        print("Please run 2_generate_sources.py first to generate sources.yml", file=sys.stderr)
        sys.exit(1)
        
    with open(SOURCES_YML) as f:
        sources = yaml.safe_load(f)

    total_models = 0
    skipped_models = 0
    models_by_domain = {'commissioning': 0, 'olids': 0, 'shared': 0}
    
    for source in sources['sources']:
        source_name = source['name']
        
        # Get staging prefix and domain from mappings
        if source_name in mappings:
            mapping = mappings[source_name]
            prefix = mapping.get('staging_prefix', f'stg_{source_name}')
            domain = mapping.get('domain', 'commissioning')  # Default to commissioning if not specified
        else:
            prefix = f'stg_{source_name}'
            domain = 'commissioning'  # Default
            print(f"Warning: No mapping found for source '{source_name}', using defaults")
        
        # Set staging directory based on domain
        if domain == 'shared':
            staging_dir = os.path.join(PROJECT_DIR, 'models', 'shared', 'staging')
        else:
            staging_dir = os.path.join(PROJECT_DIR, 'models', domain, 'staging')
        
        os.makedirs(staging_dir, exist_ok=True)

        for table in source['tables']:
            # Keep original case for source reference
            table_name = table['name']
            # Use sanitised name for file names  
            table_name_safe = sanitise_filename(table_name)
            columns = [col['name'] for col in table.get('columns', [])]
            if not columns:
                continue  # Skip tables with no columns listed

            # Quote source columns and use safe column names
            column_mappings = []
            for col in columns:
                safe_col = sanitise_column_name(col)
                column_mappings.append(f'"{col}" as {safe_col}')
            
            column_list = ',\n    '.join(column_mappings)
            
            # Add description if available
            description_comment = ""
            if source.get('description'):
                description_comment = f"-- Description: {source.get('description')}\n"
            
            # Determine if we should use base layer or source
            if source_name == 'olids_core':
                # Special handling for PATIENT table - use base_olids_patient
                if table_name.upper() == 'PATIENT':
                    base_model_name = 'base_olids_patient'
                else:
                    base_model_name = f'base_olids_{table_name_safe}'
                
                # Use base layer for OLIDS core tables
                from_clause = f"{{{{ ref('{base_model_name}') }}}}"
                source_comment = f"-- Base layer: {base_model_name} (filtered for NCL practices, excludes sensitive patients)"
            else:
                # Use source for all other tables
                from_clause = f"{{{{ source('{source_name}', '{table_name}') }}}}"
                source_comment = f"-- Source: {source['database']}.{source['schema']}"
            
            model_sql = f"""-- Staging model for {source_name}.{table_name}
{source_comment}
{description_comment}
select
    {column_list}
from {from_clause}"""

            # Create model name with prefix and safe table name
            model_name = f"{prefix}_{table_name_safe}"
            out_path = os.path.join(staging_dir, f'{model_name}.sql')
            
            # Check if this model should be preserved
            if model_name in preserved_models or table_name.upper() in preserved_tables:
                skipped_models += 1
                print(f"  Preserving manually modified model: {model_name}.sql")
                continue

            with open(out_path, 'w') as out_f:
                out_f.write(model_sql + '\n')

            total_models += 1
            models_by_domain[domain] += 1
            print(f"Created {domain} staging model: {model_name}.sql")

    print(f"\nTotal staging models created: {total_models}")
    if skipped_models > 0:
        print(f"Preserved models skipped: {skipped_models}")
    print(f"Models by domain:")
    for domain, count in models_by_domain.items():
        if count > 0:
            print(f"  - {domain}: {count} models")
    
    # Auto-generate schema files if requested
    if args.auto_schema:
        print(f"\nGenerating schema.yml files with default tests...")
        generate_schema_files(models_by_domain)
        print(f"Schema files generated with 'all_source_columns_in_staging' tests")
    
    print(f"\nNext steps:")
    if not args.auto_schema:
        print(f"  1. Generate schema files: python scripts/sources/4_generate_staging_schema.py")
        print(f"  2. Review generated staging models in models/<domain>/staging/")
    else:
        print(f"  1. Review generated staging models and schema files in models/<domain>/staging/")
    print(f"  2. Run dbt to test the models: dbt run --select staging")
    print(f"  3. Begin building your transformation models in modelling/ directories")

if __name__ == '__main__':
    main()