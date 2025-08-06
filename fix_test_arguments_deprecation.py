#!/usr/bin/env python3
"""
Script to fix dbt test argument deprecation warnings.

Converts custom generic tests from old format:
  - cluster_ids_exist:
      cluster_ids: value

To new format:
  - cluster_ids_exist:
      arguments:
        cluster_ids: value

Also handles alternative test_name format.
"""

import os
import re
import yaml
from pathlib import Path

def fix_custom_generic_tests(file_path: str, dry_run: bool = False) -> bool:
    """Fix custom generic test arguments in a YAML file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Pattern 1: cluster_ids_exist test with direct arguments
        # Matches: - cluster_ids_exist:\n      cluster_ids: value
        # Very specific pattern to avoid false matches
        pattern1 = r'(\s+- cluster_ids_exist:)\s*\n(\s+)(cluster_ids:\s*[^\n]+)'
        
        def replace_cluster_ids_exist(match):
            test_declaration = match.group(1)
            param_indent = match.group(2)
            cluster_ids_line = match.group(3)
            
            # Ensure we maintain exact indentation
            arguments_indent = param_indent
            new_param_indent = param_indent + "  "  # Add 2 spaces for proper YAML nesting
            
            return f"{test_declaration}\n{arguments_indent}arguments:\n{new_param_indent}{cluster_ids_line}"
        
        content = re.sub(pattern1, replace_cluster_ids_exist, content)
        
        # Pattern 2: dbt_utils.unique_combination_of_columns with direct parameters
        # This is a very common pattern that needs arguments wrapper
        pattern2 = r'(\s+- dbt_utils\.unique_combination_of_columns:)\s*\n(\s*\n)?(\s+)(combination_of_columns:(?:\n\s+- [^\n]+)+)(\n\s+name:\s*[^\n]+)?'
        
        def replace_unique_combination(match):
            test_declaration = match.group(1)
            empty_line = match.group(2) or ""
            param_indent = match.group(3)
            combination_section = match.group(4)
            name_section = match.group(5) or ""
            
            # Skip if this already has arguments wrapper (check the content)
            if 'arguments:' in combination_section:
                return match.group(0)
            
            arguments_indent = param_indent
            new_param_indent = param_indent + "  "
            
            # Re-indent the combination_of_columns section
            new_combination_section = ""
            for line in combination_section.split('\n'):
                if line.strip():
                    content = line.strip()
                    new_combination_section += f"{new_param_indent}{content}\n"
            
            # Handle name parameter if present
            new_name_section = ""
            if name_section:
                name_content = name_section.strip().replace('\n', '').strip()
                new_name_section = f"{new_param_indent}{name_content}\n"
            
            result = f"{test_declaration}\n{empty_line}{arguments_indent}arguments:\n{new_combination_section.rstrip()}{new_name_section}"
            return result
        
        content = re.sub(pattern2, replace_unique_combination, content)
        
        if content != original_content:
            # Validate YAML structure before writing
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
                print("Skipping this file to avoid corruption")
                return False
        else:
            return False
            
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def test_single_file(file_path: str):
    """Test the script on a single file first."""
    print(f"Testing on single file: {file_path}")
    
    # Show before
    print("\nBefore:")
    with open(file_path, 'r') as f:
        content = f.read()
        lines = content.split('\n')
        for i, line in enumerate(lines[1:15], 1):  # Show first 15 lines
            print(f"{i:3d}: {line}")
    
    # Test dry run first
    print(f"\nDry run results:")
    fix_custom_generic_tests(file_path, dry_run=True)
    
    # Actually apply the changes and show the result
    print(f"\nApplying changes...")
    fix_custom_generic_tests(file_path, dry_run=False)
    
    print("\nAfter:")
    with open(file_path, 'r') as f:
        content = f.read()
        lines = content.split('\n')
        for i, line in enumerate(lines[1:15], 1):  # Show first 15 lines
            print(f"{i:3d}: {line}")

def main():
    """Process all YAML files in the project."""
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == 'test':
        # Test mode - just test on one file
        test_file = 'models/intermediate/diagnoses/int_gestational_diabetes_diagnoses_all.yml'
        test_single_file(test_file)
        return
    
    root_dir = Path('.')
    yaml_files = list(root_dir.glob('**/*.yml'))
    
    print(f"Found {len(yaml_files)} YAML files to process...")
    
    files_modified = 0
    
    for yaml_file in yaml_files:
        if fix_custom_generic_tests(str(yaml_file), dry_run=False):
            files_modified += 1
    
    print(f"\nCompleted! Modified {files_modified} files.")

if __name__ == "__main__":
    main()