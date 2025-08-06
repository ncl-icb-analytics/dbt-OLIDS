#!/usr/bin/env python3
"""
Simple fix for dbt test format deprecation warnings.

This script moves model-level tests that appear before the description to after the description.
It uses simple regex patterns to avoid breaking YAML structure.
"""

import os
import re
import argparse
from typing import List


def find_yaml_files(directory: str) -> List[str]:
    """Find all YAML files in the models directory."""
    yaml_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(('.yml', '.yaml')):
                yaml_files.append(os.path.join(root, file))
    return yaml_files


def fix_yaml_content(content: str) -> str:
    """Fix YAML content by moving tests after description using regex."""
    # Pattern to match: model name, tests block, then description
    # This pattern captures the structure and reorders it
    pattern = r"""
        (- name:\s+\w+\s*\n)      # Model name line (group 1)
        (\s+tests:\s*\n           # Tests line (group 2)
         (?:\s+- .*\n)*           # Test items
         (?:\s+\S.*\n)*?)         # Any other test content
        (\s+description:.*)       # Description and everything after (group 3)
    """
    
    def reorder_match(match):
        model_name = match.group(1)
        tests_block = match.group(2)
        description_and_rest = match.group(3)
        
        # Find end of description to insert tests after it
        desc_lines = description_and_rest.split('\n')
        
        # Find where the description ends (look for next property at same indent level)
        desc_indent = None
        desc_end_idx = len(desc_lines)
        
        for i, line in enumerate(desc_lines):
            if line.strip().startswith('description:'):
                desc_indent = len(line) - len(line.lstrip())
            elif (desc_indent is not None and 
                  line.strip() and 
                  len(line) - len(line.lstrip()) <= desc_indent and
                  not line.strip().startswith(("'", '"', '•', '-')) and
                  ':' in line):
                desc_end_idx = i
                break
        
        # Split description and remaining content
        desc_part = '\n'.join(desc_lines[:desc_end_idx])
        remaining_part = '\n'.join(desc_lines[desc_end_idx:]) if desc_end_idx < len(desc_lines) else ''
        
        # Reorder: name, description, tests, remaining
        result = model_name + desc_part
        if remaining_part:
            result += '\n' + tests_block.rstrip() + '\n' + remaining_part
        else:
            result += '\n' + tests_block.rstrip()
        
        return result
    
    # Apply the fix
    fixed_content = re.sub(pattern, reorder_match, content, flags=re.VERBOSE | re.DOTALL)
    
    return fixed_content


def process_file(file_path: str, dry_run: bool = False) -> bool:
    """Process a single YAML file to fix deprecated test format."""
    print(f"Processing: {file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except UnicodeDecodeError:
        with open(file_path, 'r', encoding='latin-1') as f:
            content = f.read()
    
    # Check if file has the deprecated pattern
    deprecated_pattern = r'- name:\s+\w+\s*\n\s+tests:\s*\n.*?\n\s+description:'
    if not re.search(deprecated_pattern, content, re.DOTALL):
        print(f"  No deprecated format found")
        return False
    
    print(f"  Found deprecated test format - fixing...")
    
    # Fix the structure
    fixed_content = fix_yaml_content(content)
    
    if not dry_run:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(fixed_content)
        print(f"  Fixed")
    else:
        print(f"  Would fix (dry run)")
    
    return True


def main():
    parser = argparse.ArgumentParser(description='Fix dbt test format deprecation warnings')
    parser.add_argument('--directory', '-d', default='models', 
                       help='Directory to search for YAML files (default: models)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be changed without making changes')
    parser.add_argument('--file', '-f', help='Process a specific file instead of directory')
    
    args = parser.parse_args()
    
    if args.file:
        yaml_files = [args.file]
    else:
        if not os.path.exists(args.directory):
            print(f"Directory {args.directory} does not exist")
            return 1
        yaml_files = find_yaml_files(args.directory)
    
    if not yaml_files:
        print("No YAML files found")
        return 0
    
    print(f"Found {len(yaml_files)} YAML files")
    
    fixed_count = 0
    for file_path in yaml_files:
        try:
            if process_file(file_path, args.dry_run):
                fixed_count += 1
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
    
    print(f"\nProcessing complete:")
    print(f"  Files processed: {len(yaml_files)}")
    print(f"  Files {'would be ' if args.dry_run else ''}fixed: {fixed_count}")
    
    return 0


if __name__ == '__main__':
    exit(main())