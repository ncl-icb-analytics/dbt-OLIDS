#!/usr/bin/env python3
"""
Generate schema.yml files for all staging models with default tests.
This script scans the domain-based staging directories and creates schema.yml files
with 'all_source_columns_in_staging' tests for each staging model.
"""

import os
import sys
import yaml
from pathlib import Path

# Path configuration
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(CURRENT_DIR)  # scripts directory
PROJECT_DIR = os.path.dirname(SCRIPTS_DIR)  # actual project root
MODEL_TESTS_FILE = os.path.join(CURRENT_DIR, 'default_model_tests.yml')

# Domain directories
DOMAINS = ['olids', 'shared']

def load_model_tests():
    """Load default model tests configuration from YAML file"""
    if not os.path.exists(MODEL_TESTS_FILE):
        print(f"Warning: Model tests file not found at {MODEL_TESTS_FILE}, using hardcoded defaults")
        return {'default_tests': ['all_source_columns_in_staging'], 'model_specific_tests': {}}
        
    with open(MODEL_TESTS_FILE, 'r') as f:
        return yaml.safe_load(f)

def generate_schema_for_domain(domain):
    """Generate schema.yml file for a specific domain."""
    staging_dir = Path(PROJECT_DIR) / 'models' / domain / 'staging'
    schema_file = staging_dir / 'schema.yml'
    
    if not staging_dir.exists():
        print(f"Warning: Staging directory not found: {staging_dir}")
        return 0
    
    # Get all staging model files
    staging_models = []
    for sql_file in staging_dir.glob('*.sql'):
        if sql_file.name.startswith('stg_'):
            model_name = sql_file.stem  # Get filename without extension
            staging_models.append(model_name)
    
    if not staging_models:
        print(f"No staging models found in {domain}/staging/")
        return 0
    
    # Load test configuration
    test_config = load_model_tests()
    default_tests = test_config.get('default_tests', ['all_source_columns_in_staging'])
    model_specific_tests = test_config.get('model_specific_tests', {}) or {}
    
    # Generate schema content
    print(f"Generating schema.yml for {domain} domain with {len(staging_models)} models...")
    
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
    return len(staging_models)

def main():
    """Generate schema.yml files for all domain staging directories."""
    print("Generating schema.yml files for staging models...")
    print("=" * 50)
    
    total_models = 0
    domains_processed = 0
    
    for domain in DOMAINS:
        model_count = generate_schema_for_domain(domain)
        if model_count > 0:
            total_models += model_count
            domains_processed += 1
    
    print("=" * 50)
    print(f"Schema generation complete!")
    print(f"  - Domains processed: {domains_processed}")
    print(f"  - Total models: {total_models}")
    print(f"  - Default test applied: all_source_columns_in_staging")
    
    if total_models > 0:
        print(f"\nNext steps:")
        print(f"  1. Review generated schema files in models/<domain>/staging/")
        print(f"  2. Run dbt to test the models: dbt run --select staging")
        print(f"  3. Run tests: dbt test --select staging")
    else:
        print(f"\nNo staging models found in any domain directories.")
        print(f"Make sure you have run the staging model generation first:")
        print(f"  python scripts/sources/3_generate_staging_models.py")

if __name__ == '__main__':
    main()