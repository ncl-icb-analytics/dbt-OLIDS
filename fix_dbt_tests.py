#!/usr/bin/env python3
"""
Fix dbt test format deprecation warnings across all YAML files.
Moves model-level tests from before description to after description.
"""

import os
import re
import argparse
from pathlib import Path


def find_yaml_files(directory: str) -> list[str]:
    """Find all YAML files in the models directory."""
    yaml_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(('.yml', '.yaml')):
                yaml_files.append(os.path.join(root, file))
    return yaml_files


def fix_yaml_file(file_path: str, dry_run: bool = False) -> bool:
    """Fix deprecated test format in a single YAML file."""
    print(f"Processing: {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Check if file has deprecated pattern (tests before description)
    has_issue = False
    lines = content.split('\n')
    
    for i, line in enumerate(lines):
        if re.match(r'^- name:\s+\w+', line):
            # Found model definition, check next few lines
            j = i + 1
            found_tests = False
            found_description = False
            
            while j < len(lines) and j < i + 10:  # Check next 10 lines
                if re.match(r'^\s+tests:\s*$', lines[j]):
                    found_tests = True
                elif re.match(r'^\s+description:', lines[j]):
                    found_description = True
                    break
                elif re.match(r'^- name:', lines[j]):  # Next model
                    break
                j += 1
            
            if found_tests and found_description:
                has_issue = True
                break
    
    if not has_issue:
        print("  No deprecated format found")
        return False
    
    print("  Found deprecated test format - fixing...")
    
    # Apply the fix using regex
    # Pattern: model name, tests block, description + content
    pattern = r"""
        (^- name:\s+\w+\s*\n)                    # Model name line
        (\s+tests:\s*\n                         # Tests block
         (?:\s+- [^\n]*\n                       # Test items
          (?:\s+[^\n]*:\s*[^\n]*\n)*)*          # Test parameters
        )
        (\s+description:\s*[^\n]*\n             # Description line
         (?:(?!\s+\w+:)[^\n]*\n)*               # Description content (until next property)
        )
    """
    
    def reorder_tests(match):
        name_line = match.group(1)
        tests_block = match.group(2)
        description_block = match.group(3)
        
        # Return: name, description, tests
        return name_line + description_block + tests_block
    
    fixed_content = re.sub(pattern, reorder_tests, content, flags=re.VERBOSE | re.MULTILINE)
    
    if fixed_content != content:
        if not dry_run:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(fixed_content)
            print("  Fixed")
        else:
            print("  Would fix (dry run)")
        return True
    else:
        print("  No changes needed")
        return False


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
            if fix_yaml_file(file_path, args.dry_run):
                fixed_count += 1
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
    
    print(f"\nProcessing complete:")
    print(f"  Files processed: {len(yaml_files)}")
    print(f"  Files {'would be ' if args.dry_run else ''}fixed: {fixed_count}")
    
    return 0


if __name__ == '__main__':
    exit(main())