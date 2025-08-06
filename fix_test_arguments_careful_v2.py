#!/usr/bin/env python3
"""
Very careful script to fix dbt test argument deprecation warnings.
This version is extremely precise to avoid any YAML structural issues.
"""

import os
import re
import yaml
from pathlib import Path

def fix_cluster_ids_exist_tests(content: str) -> str:
    """Fix cluster_ids_exist tests specifically."""
    # Very precise pattern: - cluster_ids_exist:\n      cluster_ids: value
    pattern = r'(\s+- cluster_ids_exist:)\s*\n(\s+)(cluster_ids:\s*[^\n]+)'
    
    def replace_cluster_ids(match):
        test_declaration = match.group(1)
        param_indent = match.group(2)
        cluster_ids_line = match.group(3)
        
        # arguments: goes at the same indent as cluster_ids was
        arguments_indent = param_indent
        # cluster_ids: gets indented 2 more spaces
        new_param_indent = param_indent + "  "
        
        return f"{test_declaration}\n{arguments_indent}arguments:\n{new_param_indent}{cluster_ids_line}"
    
    return re.sub(pattern, replace_cluster_ids, content)

def fix_dbt_utils_unique_combination_tests(content: str) -> str:
    """Fix dbt_utils.unique_combination_of_columns tests specifically."""
    # Pattern: - dbt_utils.unique_combination_of_columns:\n\n      combination_of_columns:
    pattern = r'(\s+- dbt_utils\.unique_combination_of_columns:)(\s*\n\s*\n)?(\s+)(combination_of_columns:(?:\n\s+- [^\n]+)+)(\n\s+name:\s*[^\n]+)?'
    
    def replace_unique_combination(match):
        test_declaration = match.group(1)
        empty_lines = match.group(2) or ""
        param_indent = match.group(3)
        combination_section = match.group(4)
        name_section = match.group(5) or ""
        
        # Skip if already has arguments
        if 'arguments:' in combination_section:
            return match.group(0)
        
        arguments_indent = param_indent
        new_param_indent = param_indent + "  "
        
        # Process combination_of_columns section
        new_combination = ""
        for line in combination_section.split('\n'):
            if line.strip():
                content_text = line.strip()
                new_combination += f"{new_param_indent}{content_text}\n"
        
        # Process name section if present
        new_name = ""
        if name_section:
            name_content = name_section.strip().replace('\n', '').strip()
            new_name = f"{new_param_indent}{name_content}"
        
        result = f"{test_declaration}\n{empty_lines}{arguments_indent}arguments:\n{new_combination.rstrip()}"
        if new_name:
            result += f"\n{new_name}"
        
        return result
    
    return re.sub(pattern, replace_unique_combination, content)

def fix_other_dbt_utils_tests(content: str) -> str:
    """Fix other dbt_utils tests that need arguments wrapper."""
    # List of dbt_utils tests that need arguments wrapper
    test_names = [
        'dbt_utils.expression_is_true',
        'dbt_utils.at_least_one',
        'dbt_utils.accepted_range',
        'dbt_utils.not_accepted_values'
    ]
    
    for test_name in test_names:
        # Pattern: - test_name:\n      param1: value1\n      param2: value2
        pattern = rf'(\s+- {re.escape(test_name)}:)\s*\n((?:\s+[a-zA-Z_][a-zA-Z0-9_]*:\s*[^\n]*\n)+)'
        
        def replace_test(match):
            test_declaration = match.group(1)
            params_section = match.group(2)
            
            # Skip if already has arguments
            if 'arguments:' in params_section:
                return match.group(0)
            
            # Find the base indentation of the first parameter
            first_param_line = params_section.split('\n')[0]
            param_indent_size = len(first_param_line) - len(first_param_line.lstrip())
            param_indent = ' ' * param_indent_size
            
            arguments_indent = param_indent
            new_param_indent = param_indent + "  "
            
            # Rebuild parameters with new indentation
            new_params = ""
            for line in params_section.split('\n'):
                if line.strip():
                    param_content = line.strip()
                    new_params += f"{new_param_indent}{param_content}\n"
            
            return f"{test_declaration}\n{arguments_indent}arguments:\n{new_params.rstrip()}"
        
        content = re.sub(pattern, replace_test, content, flags=re.MULTILINE)
    
    return content

def fix_test_arguments_in_file(file_path: str, dry_run: bool = False) -> bool:
    """Fix test arguments in a single YAML file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Apply fixes in sequence
        content = fix_cluster_ids_exist_tests(content)
        content = fix_dbt_utils_unique_combination_tests(content)
        content = fix_other_dbt_utils_tests(content)
        
        if content != original_content:
            # Validate YAML before writing
            try:
                yaml.safe_load(content)
                if not dry_run:
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(content)
                    print(f"Fixed test arguments in {file_path}")
                else:
                    print(f"Would fix test arguments in {file_path}")
                return True
            except yaml.YAMLError as ye:
                print(f"WARNING: YAML validation failed for {file_path}: {ye}")
                print("Skipping to avoid corruption")
                return False
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
    
    print("\nBefore:")
    with open(file_path, 'r') as f:
        lines = f.readlines()
        for i, line in enumerate(lines[:20], 1):
            print(f"{i:3d}: {line.rstrip()}")
    
    print(f"\nDry run:")
    fix_test_arguments_in_file(file_path, dry_run=True)
    
    print(f"\nApplying changes...")
    result = fix_test_arguments_in_file(file_path, dry_run=False)
    
    if result:
        print("\nAfter:")
        with open(file_path, 'r') as f:
            lines = f.readlines()
            for i, line in enumerate(lines[:20], 1):
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
        if fix_test_arguments_in_file(str(yaml_file)):
            files_modified += 1
    
    print(f"\nCompleted! Modified {files_modified} files.")

if __name__ == "__main__":
    main()