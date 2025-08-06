#!/usr/bin/env python3
"""
Fix dbt test format deprecation warnings across all YAML files.

This script converts tests from the deprecated top-level format to the proper nested structure.
It handles tests that appear at the model level before the description, moving them to 
the end of the model definition.
"""

import os
import re
import argparse
from pathlib import Path
from typing import List


def find_yaml_files(directory: str) -> List[str]:
    """Find all YAML files in the models directory."""
    yaml_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(('.yml', '.yaml')):
                yaml_files.append(os.path.join(root, file))
    return yaml_files


def has_deprecated_test_format(content: str) -> bool:
    """Check if content has deprecated test format (tests before description)."""
    # Look for pattern: model name, then tests block, then description
    pattern = r'- name:\s+\w+.*?\n\s+tests:\s*\n.*?\n\s+description:'
    return bool(re.search(pattern, content, re.DOTALL))


def fix_yaml_content(content: str) -> str:
    """Fix YAML content by moving tests after description/columns."""
    lines = content.split('\n')
    fixed_lines = []
    i = 0
    
    while i < len(lines):
        line = lines[i]
        
        # Look for model definition
        if re.match(r'^- name:\s+\w+', line):
            # Start of a model - collect the entire model
            model_start = i
            model_indent = len(line) - len(line.lstrip())
            
            # Find the end of this model
            j = i + 1
            while j < len(lines):
                current_line = lines[j]
                if current_line.strip() == '':
                    j += 1
                    continue
                
                current_indent = len(current_line) - len(current_line.lstrip())
                
                # If we hit another model or something at the same level, break
                if (current_indent <= model_indent and 
                    (current_line.startswith('- name:') or 
                     not current_line.startswith(' '))):
                    break
                j += 1
            
            # Process this model
            model_lines = lines[i:j]
            fixed_model = fix_single_model(model_lines)
            fixed_lines.extend(fixed_model)
            i = j
            continue
        
        fixed_lines.append(line)
        i += 1
    
    return '\n'.join(fixed_lines)


def fix_single_model(model_lines: List[str]) -> List[str]:
    """Fix a single model's test format."""
    if not model_lines:
        return model_lines
    
    # Find tests block
    tests_start = None
    tests_end = None
    description_line = None
    
    for i, line in enumerate(model_lines):
        if re.match(r'^\s+tests:\s*$', line):
            tests_start = i
        elif re.match(r'^\s+description:', line):
            description_line = i
            if tests_start is not None and tests_end is None:
                # Find end of tests block
                for j in range(tests_start + 1, len(model_lines)):
                    test_line = model_lines[j]
                    if (test_line.strip() and 
                        not test_line.startswith('  ') and 
                        not test_line.startswith('    -') and
                        not test_line.startswith('      ')):
                        tests_end = j
                        break
                if tests_end is None:
                    tests_end = description_line
    
    # If tests come before description, move them
    if (tests_start is not None and 
        description_line is not None and 
        tests_start < description_line):
        
        # Extract tests block
        if tests_end is None:
            tests_end = len(model_lines)
        
        tests_block = model_lines[tests_start:tests_end]
        
        # Remove tests from original position
        remaining_lines = (model_lines[:tests_start] + 
                          model_lines[tests_end:])
        
        # Find where to insert tests (after columns if they exist, otherwise at end)
        insert_pos = len(remaining_lines)
        for i in range(len(remaining_lines) - 1, -1, -1):
            if remaining_lines[i].strip():
                insert_pos = i + 1
                break
        
        # Insert tests at the end
        result = (remaining_lines[:insert_pos] + 
                 tests_block + 
                 remaining_lines[insert_pos:])
        
        return result
    
    return model_lines


def process_file(file_path: str, dry_run: bool = False) -> bool:
    """Process a single YAML file to fix deprecated test format."""
    print(f"Processing: {file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except UnicodeDecodeError:
        # Try with different encoding
        with open(file_path, 'r', encoding='latin-1') as f:
            content = f.read()
    
    if not has_deprecated_test_format(content):
        print(f"  No deprecated format found")
        return False
    
    print(f"  Found deprecated test format - fixing...")
    
    # Fix the structure
    fixed_content = fix_yaml_content(content)
    
    if not dry_run:
        # Write the fixed content
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
        # Process single file
        yaml_files = [args.file]
    else:
        # Find all YAML files
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