#!/usr/bin/env python3
"""
Fix all test argument deprecations (MissingArgumentsPropertyInGenericTestDeprecation).
Handles various test types including dbt_utils tests and built-in tests.
"""

import os
import re
import yaml
from typing import Dict, List, Set


def find_yaml_files_with_tests(directory: str = 'models') -> List[str]:
    """Find all YAML files that contain tests."""
    yaml_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(('.yml', '.yaml')):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        # Check if file contains any tests
                        if 'tests:' in content or '- name:' in content:
                            yaml_files.append(file_path)
                except:
                    continue
    return yaml_files


def find_deprecated_test_patterns(content: str) -> List[Dict]:
    """Find all deprecated test argument patterns in YAML content."""
    deprecated_patterns = []
    
    # Common test patterns that need arguments wrapping
    # Pattern 1: dbt_utils tests with direct arguments
    dbt_utils_pattern = r'(\s+- )(dbt_utils\.\w+):\s*\n((?:\s+\w+:\s*[^\n]*\n)*)'
    
    for match in re.finditer(dbt_utils_pattern, content):
        test_indent = match.group(1)
        test_name = match.group(2)
        args_block = match.group(3)
        
        # Parse arguments to see if they're direct (not under 'arguments:')
        if args_block and not args_block.strip().startswith('arguments:'):
            deprecated_patterns.append({
                'type': 'dbt_utils',
                'match': match,
                'test_name': test_name,
                'args_block': args_block,
                'indent': test_indent
            })
    
    # Pattern 2: Generic tests with direct arguments (like unique_combination_of_columns)
    generic_test_pattern = r'(\s+- )(\w+):\s*\n((?:\s+\w+:\s*[^\n]*\n)*)'
    
    for match in re.finditer(generic_test_pattern, content):
        test_indent = match.group(1)
        test_name = match.group(2)
        args_block = match.group(3)
        
        # Skip if this is already a dbt_utils test (handled above)
        if 'dbt_utils.' in test_name:
            continue
            
        # Skip built-in tests without arguments (not_null, unique, etc.)
        if test_name in ['not_null', 'unique', 'accepted_values', 'relationships']:
            continue
            
        # Check if this has arguments that should be wrapped
        if args_block and not args_block.strip().startswith('arguments:'):
            # Common generic test names that need wrapping
            if test_name in ['unique_combination_of_columns', 'cluster_ids_exist', 
                           'bnf_codes_exist', 'expression_is_true', 'accepted_range']:
                deprecated_patterns.append({
                    'type': 'generic',
                    'match': match,
                    'test_name': test_name,
                    'args_block': args_block,
                    'indent': test_indent
                })
    
    return deprecated_patterns


def fix_deprecated_test_arguments(content: str) -> str:
    """Fix all deprecated test argument patterns in content."""
    fixed_content = content
    deprecated_patterns = find_deprecated_test_patterns(content)
    
    # Process in reverse order to avoid offset issues
    for pattern in reversed(deprecated_patterns):
        match = pattern['match']
        test_indent = pattern['indent']
        test_name = pattern['test_name']
        args_block = pattern['args_block']
        
        # Create the fixed version with arguments wrapper
        base_indent = len(test_indent)
        args_indent = ' ' * (base_indent + 2)  # Add 2 spaces for arguments:
        param_indent = ' ' * (base_indent + 4)  # Add 4 spaces for parameters
        
        # Reindent the arguments block
        fixed_args = []
        for line in args_block.split('\n'):
            if line.strip():
                # Remove existing indentation and add new parameter indentation
                clean_line = line.lstrip()
                fixed_args.append(param_indent + clean_line)
        
        # Build the fixed test block
        fixed_test = (test_indent + test_name + ':\n' +
                     args_indent + 'arguments:\n' +
                     '\n'.join(fixed_args) + '\n')
        
        # Replace in content
        original_text = match.group(0).rstrip('\n')
        fixed_content = fixed_content.replace(original_text, fixed_test.rstrip('\n'), 1)
    
    return fixed_content


def process_yaml_file(file_path: str, dry_run: bool = False) -> bool:
    """Process a single YAML file to fix deprecated test arguments."""
    print(f"Processing: {file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        print(f"  Error reading file: {e}")
        return False
    
    # Find deprecated patterns
    deprecated_patterns = find_deprecated_test_patterns(content)
    
    if not deprecated_patterns:
        print("  No deprecated test arguments found")
        return False
    
    print(f"  Found {len(deprecated_patterns)} deprecated test argument(s)")
    for pattern in deprecated_patterns:
        print(f"    - {pattern['test_name']}")
    
    if dry_run:
        print("  Would fix (dry run)")
        return True
    
    # Apply fixes
    fixed_content = fix_deprecated_test_arguments(content)
    
    if fixed_content != content:
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(fixed_content)
            print("  Fixed")
            return True
        except Exception as e:
            print(f"  Error writing file: {e}")
            return False
    
    print("  No changes made")
    return False


def main():
    print("=== Finding YAML files with tests ===")
    yaml_files = find_yaml_files_with_tests()
    print(f"Found {len(yaml_files)} YAML files to check")
    
    # Dry run first
    print("\n=== DRY RUN ===")
    dry_run_count = 0
    for file_path in yaml_files:
        try:
            if process_yaml_file(file_path, dry_run=True):
                dry_run_count += 1
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
    
    print(f"\nDry run complete: {dry_run_count} files would be fixed")
    
    if dry_run_count == 0:
        print("No files need fixing!")
        return
    
    # Apply fixes
    print("\n=== APPLYING FIXES ===")
    fixed_count = 0
    for file_path in yaml_files:
        try:
            if process_yaml_file(file_path, dry_run=False):
                fixed_count += 1
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
    
    print(f"\nFix complete: {fixed_count} files fixed")


if __name__ == '__main__':
    main()