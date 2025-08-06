#!/usr/bin/env python3
"""
Simple and safe fix for dbt test deprecation warnings.
Uses targeted regex to swap only the specific pattern: tests before description.
"""

import os
import re
import argparse


def fix_yaml_file(file_path: str, dry_run: bool = False) -> bool:
    """Fix deprecated test format using targeted regex replacement."""
    print(f"Processing: {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Very specific pattern to match and swap tests/description order
    # This pattern captures the exact structure we need to fix
    pattern = r'''
        (^- name:\s+\w+\s*\n)           # Model name line (group 1)
        (\s+tests:\s*\n                 # Tests block (group 2)
         (?:\s+- [^\n]*\n)*             # Test items and their parameters
         (?:\s+[^\n]+:[^\n]*\n)*        # Additional test config
        )
        (\s+description:\s*)             # Description start (group 3)
    '''
    
    # Check if pattern exists
    matches = re.findall(pattern, content, re.VERBOSE | re.MULTILINE)
    if not matches:
        print("  No deprecated format found")
        return False
    
    print(f"  Found {len(matches)} deprecated test format(s) - fixing...")
    
    # Replace pattern: swap tests and description
    def swap_order(match):
        model_name = match.group(1)      # - name: model_name\n
        tests_block = match.group(2)     # tests block
        description_start = match.group(3)  # description:
        
        # Return: name, description, tests
        return model_name + description_start
    
    # First pass: remove tests blocks that are before descriptions
    temp_content = re.sub(pattern, swap_order, content, flags=re.VERBOSE | re.MULTILINE)
    
    # Second pass: add tests blocks after descriptions end
    # Find where each description ends and insert the tests there
    stored_tests = []
    
    def store_tests(match):
        stored_tests.append(match.group(2))  # Store the tests block
        return match.group(1) + match.group(3)  # Return name + description start
    
    # Store tests and remove them
    temp_content = re.sub(pattern, store_tests, content, flags=re.VERBOSE | re.MULTILINE)
    
    # Now find where to insert the stored tests (after description ends)
    description_end_pattern = r"(\n\s+description:\s*[^\n]*(?:\n(?!\s+\w+:)[^\n]*)*)\n(\s+)"
    
    def insert_tests(match):
        if stored_tests:
            tests_block = stored_tests.pop(0)
            description_part = match.group(1)
            indent = match.group(2)
            return description_part + "\n" + tests_block + indent
        return match.group(0)
    
    fixed_content = re.sub(description_end_pattern, insert_tests, temp_content)
    
    if fixed_content != content:
        if not dry_run:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(fixed_content)
            print("  Fixed")
        else:
            print("  Would fix (dry run)")
        return True
    else:
        print("  No changes made")
        return False


def main():
    parser = argparse.ArgumentParser(description='Fix dbt test format deprecation warnings')
    parser.add_argument('--file', '-f', required=True, help='Process a specific file')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be changed')
    
    args = parser.parse_args()
    
    try:
        result = fix_yaml_file(args.file, args.dry_run)
        return 0 if result else 1
    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == '__main__':
    exit(main())