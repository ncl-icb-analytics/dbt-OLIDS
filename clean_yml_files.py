#!/usr/bin/env python3
"""
Clean YAML files by preserving only essential elements:
- Model name and description (with formatting)
- cluster_ids_exist tests (in new arguments format)
- Remove all other tests
"""

import yaml
import os
from pathlib import Path
from ruamel.yaml import YAML
from ruamel.yaml.scalarstring import LiteralScalarString

def clean_yaml_file(file_path: str, dry_run: bool = False) -> bool:
    """Clean a single YAML file."""
    try:
        # Use ruamel.yaml to preserve formatting and comments
        yaml_handler = YAML()
        yaml_handler.preserve_quotes = True
        yaml_handler.width = 4096  # Prevent line wrapping
        
        with open(file_path, 'r', encoding='utf-8') as f:
            data = yaml_handler.load(f)
        
        if not data or 'models' not in data:
            return False
        
        original_content = str(data)
        models_modified = False
        
        for model in data['models']:
            if not isinstance(model, dict) or 'name' not in model:
                continue
            
            # Preserve name and description
            cleaned_model = {
                'name': model['name']
            }
            
            # Preserve description with exact formatting
            if 'description' in model:
                cleaned_model['description'] = model['description']
            
            # Handle cluster_ids_exist tests at model level
            if 'tests' in model:
                cluster_tests = []
                for test in model['tests']:
                    if isinstance(test, dict) and 'cluster_ids_exist' in test:
                        # Convert to new format with arguments
                        cluster_ids_value = test['cluster_ids_exist']
                        if isinstance(cluster_ids_value, dict):
                            # Already has some structure
                            if 'cluster_ids' in cluster_ids_value:
                                cluster_ids = cluster_ids_value['cluster_ids']
                            else:
                                continue  # Skip malformed test
                        else:
                            # Direct value
                            cluster_ids = cluster_ids_value
                        
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
            
            # Handle columns (preserve name and description only, no tests)
            if 'columns' in model:
                cleaned_columns = []
                for column in model['columns']:
                    if isinstance(column, dict) and 'name' in column:
                        cleaned_column = {
                            'name': column['name']
                        }
                        
                        # Preserve description with exact formatting
                        if 'description' in column:
                            cleaned_column['description'] = column['description']
                        
                        cleaned_columns.append(cleaned_column)
                
                if cleaned_columns:
                    cleaned_model['columns'] = cleaned_columns
            
            # Replace the model in the original structure
            model_index = data['models'].index(model)
            data['models'][model_index] = cleaned_model
            models_modified = True
        
        if models_modified:
            if not dry_run:
                with open(file_path, 'w', encoding='utf-8') as f:
                    yaml_handler.dump(data, f)
                print(f"Cleaned {file_path}")
            else:
                print(f"Would clean {file_path}")
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
    
    print("\nBefore (first 30 lines):")
    with open(file_path, 'r') as f:
        lines = f.readlines()
        for i, line in enumerate(lines[:30], 1):
            print(f"{i:3d}: {line.rstrip()}")
    
    print(f"\nDry run:")
    clean_yaml_file(file_path, dry_run=True)
    
    print(f"\nApplying changes...")
    result = clean_yaml_file(file_path, dry_run=False)
    
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
    
    # Process all YAML files
    root_dir = Path('.')
    yaml_files = [f for f in root_dir.glob('**/*.yml') if not str(f).startswith('dbt_packages')]
    
    print(f"Found {len(yaml_files)} YAML files to process...")
    
    files_modified = 0
    for yaml_file in yaml_files:
        if clean_yaml_file(str(yaml_file)):
            files_modified += 1
    
    print(f"\nCompleted! Modified {files_modified} files.")

if __name__ == "__main__":
    main()