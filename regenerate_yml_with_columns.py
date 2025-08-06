#!/usr/bin/env python3
"""
Regenerate YAML files with complete column lists by:
1. Preserving model name and description (with formatting)
2. Preserving cluster_ids_exist tests (in new arguments format)
3. Automatically generating complete column list from actual model
4. Preserving existing column descriptions where they exist
5. Removing all other tests
"""

import yaml
import os
import subprocess
import json
from pathlib import Path
from ruamel.yaml import YAML

def get_model_columns(model_name: str) -> list:
    """Get actual columns from a dbt model using dbt ls command."""
    try:
        # Use dbt ls to get model information
        cmd = ['dbt', 'ls', '--select', model_name, '--resource-type', 'model', '--output', 'json']
        result = subprocess.run(cmd, capture_output=True, text=True, cwd='.')
        
        if result.returncode != 0:
            print(f"Warning: Could not get columns for {model_name}")
            return []
        
        # Parse the JSON output
        for line in result.stdout.strip().split('\n'):
            if line:
                model_info = json.loads(line)
                if model_info.get('name') == model_name:
                    # Try to get columns from the model's metadata
                    columns = model_info.get('columns', {})
                    return list(columns.keys()) if columns else []
        
        return []
        
    except Exception as e:
        print(f"Error getting columns for {model_name}: {e}")
        return []

def get_model_columns_from_compiled(model_name: str) -> list:
    """Get columns by running dbt run-operation to compile the model."""
    try:
        # Alternative: use dbt show or dbt compile to get column info
        cmd = ['dbt', 'show', '--select', model_name, '--limit', '0']
        result = subprocess.run(cmd, capture_output=True, text=True, cwd='.')
        
        if result.returncode == 0:
            # Parse the output to extract column names
            # This is a fallback method
            lines = result.stdout.split('\n')
            for line in lines:
                if '|' in line and line.strip().startswith('|'):
                    # This looks like a table header
                    columns = [col.strip() for col in line.split('|')[1:-1]]
                    return [col for col in columns if col]
        
        return []
        
    except Exception as e:
        print(f"Error getting compiled columns for {model_name}: {e}")
        return []

def regenerate_yaml_file(file_path: str, dry_run: bool = False) -> bool:
    """Regenerate a single YAML file with complete column information."""
    try:
        # Use ruamel.yaml to preserve formatting
        yaml_handler = YAML()
        yaml_handler.preserve_quotes = True
        yaml_handler.width = 4096
        
        with open(file_path, 'r', encoding='utf-8') as f:
            data = yaml_handler.load(f)
        
        if not data or 'models' not in data:
            return False
        
        models_modified = False
        
        for model in data['models']:
            if not isinstance(model, dict) or 'name' not in model:
                continue
            
            model_name = model['name']
            print(f"Processing model: {model_name}")
            
            # Build new clean model structure
            cleaned_model = {
                'name': model_name
            }
            
            # Preserve description with exact formatting
            if 'description' in model:
                cleaned_model['description'] = model['description']
            
            # Handle cluster_ids_exist tests at model level
            if 'tests' in model:
                cluster_tests = []
                for test in model['tests']:
                    if isinstance(test, dict):
                        if 'cluster_ids_exist' in test:
                            # Handle different formats
                            cluster_test_data = test['cluster_ids_exist']
                            
                            if isinstance(cluster_test_data, dict):
                                if 'arguments' in cluster_test_data:
                                    # Already in new format
                                    cluster_ids = cluster_test_data['arguments'].get('cluster_ids')
                                elif 'cluster_ids' in cluster_test_data:
                                    # Old direct format
                                    cluster_ids = cluster_test_data['cluster_ids']
                                else:
                                    continue
                            else:
                                # Direct value
                                cluster_ids = cluster_test_data
                            
                            if cluster_ids:
                                # Create new format
                                new_test = {
                                    'cluster_ids_exist': {
                                        'arguments': {
                                            'cluster_ids': cluster_ids
                                        }
                                    }
                                }
                                cluster_tests.append(new_test)
                
                if cluster_tests:
                    cleaned_model['tests'] = cluster_tests
            
            # Get existing column descriptions to preserve them
            existing_columns = {}
            if 'columns' in model:
                for col in model['columns']:
                    if isinstance(col, dict) and 'name' in col:
                        existing_columns[col['name']] = col.get('description', '')
            
            # Try to get actual columns from the model
            # First try using dbt ls (faster but may not always work)
            actual_columns = get_model_columns(model_name)
            
            # If that didn't work, try alternative method
            if not actual_columns:
                actual_columns = get_model_columns_from_compiled(model_name)
            
            # If we still don't have columns, use existing ones
            if not actual_columns:
                actual_columns = list(existing_columns.keys())
                if actual_columns:
                    print(f"  Using existing columns for {model_name}")
                else:
                    print(f"  No columns found for {model_name}")
            else:
                print(f"  Found {len(actual_columns)} columns for {model_name}")
            
            # Build columns list
            if actual_columns:
                cleaned_columns = []
                for col_name in actual_columns:
                    cleaned_column = {'name': col_name}
                    
                    # Preserve existing description if available
                    if col_name in existing_columns and existing_columns[col_name]:
                        cleaned_column['description'] = existing_columns[col_name]
                    
                    cleaned_columns.append(cleaned_column)
                
                cleaned_model['columns'] = cleaned_columns
            
            # Replace the model in the original structure
            model_index = data['models'].index(model)
            data['models'][model_index] = cleaned_model
            models_modified = True
        
        if models_modified:
            if not dry_run:
                with open(file_path, 'w', encoding='utf-8') as f:
                    yaml_handler.dump(data, f)
                print(f"Regenerated {file_path}")
            else:
                print(f"Would regenerate {file_path}")
            return True
        else:
            return False
            
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def test_single_file(file_path: str):
    """Test the script on a single file."""
    print(f"Testing on file: {file_path}")
    
    if not os.path.exists(file_path):
        print(f"File does not exist: {file_path}")
        return
    
    print("\nBefore (first 20 lines):")
    with open(file_path, 'r') as f:
        lines = f.readlines()
        for i, line in enumerate(lines[:20], 1):
            print(f"{i:3d}: {line.rstrip()}")
    
    print(f"\nDry run:")
    regenerate_yaml_file(file_path, dry_run=True)
    
    print(f"\nApplying changes...")
    result = regenerate_yaml_file(file_path, dry_run=False)
    
    if result:
        print("\nAfter (first 30 lines):")
        with open(file_path, 'r') as f:
            lines = f.readlines()
            for i, line in enumerate(lines[:30], 1):
                print(f"{i:3d}: {line.rstrip()}")
    else:
        print("No changes made.")

def main():
    """Main function."""
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == 'test':
        # Test on a specific file
        if len(sys.argv) > 2:
            test_file = sys.argv[2]
        else:
            test_file = 'models/intermediate/diagnoses/int_gestational_diabetes_diagnoses_all.yml'
        test_single_file(test_file)
        return
    
    # Process all YAML files in models directory
    root_dir = Path('models')
    yaml_files = [f for f in root_dir.glob('**/*.yml') if f.name != 'schema.yml']
    
    print(f"Found {len(yaml_files)} YAML files to process...")
    
    files_modified = 0
    for yaml_file in yaml_files:
        if regenerate_yaml_file(str(yaml_file)):
            files_modified += 1
    
    print(f"\nCompleted! Modified {files_modified} files.")

if __name__ == "__main__":
    main()