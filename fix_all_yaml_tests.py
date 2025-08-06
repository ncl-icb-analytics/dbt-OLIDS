#!/usr/bin/env python3
"""
Fix dbt test format deprecation warnings across all YAML files.
Based on the successful direct approach, this scales to handle all files.
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
    
    # Pattern to find models with tests before description
    # Look for: model name, tests block, then description
    pattern = r'(- name:\s+(\w+)\s*\n)(\s+tests:\s*\n(?:\s+- [^\n]*\n(?:\s+[^\n]*\n)*)*?)(\s+description:)'
    
    matches = list(re.finditer(pattern, content))
    
    if not matches:
        print("  No deprecated format found")
        return False
    
    print(f"  Found {len(matches)} model(s) with deprecated test format")
    
    if dry_run:
        for match in matches:
            model_name = match.group(2)
            print(f"    - {model_name}")
        print("  Would fix (dry run)")
        return True
    
    # Process matches in reverse order to avoid offset issues
    fixed_content = content
    for match in reversed(matches):
        model_name_line = match.group(1)  # - name: model_name\n
        model_name = match.group(2)       # just the model name
        tests_block = match.group(3)      # the tests block
        description_start = match.group(4) # description:
        
        print(f"    Fixing model: {model_name}")
        
        # Remove tests block from before description
        old_text = model_name_line + tests_block + description_start
        new_text = model_name_line + description_start
        
        # Find where this model's description ends
        # Look for the end of the description block (before next property like columns:, tests:, etc.)
        start_pos = match.end()
        
        # Find the end of description content
        desc_end_pattern = r"('\s*\n\n)(\s+)(?=\w+:)"  # End of quoted description + whitespace before next property
        desc_match = re.search(desc_end_pattern, fixed_content[start_pos:start_pos + 2000])
        
        if desc_match:
            # Insert tests after description ends
            desc_end_pos = start_pos + desc_match.end(1)
            indent = desc_match.group(2)
            
            # Insert the tests block
            tests_to_insert = tests_block + "\n"
            fixed_content = (fixed_content[:desc_end_pos] + 
                           tests_to_insert + 
                           fixed_content[desc_end_pos:])
        
        # Now replace the original pattern
        fixed_content = fixed_content.replace(old_text, new_text, 1)
    
    # Write the fixed content
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(fixed_content)
    
    print("  Fixed")
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
    total_models_fixed = 0
    
    for file_path in yaml_files:
        try:
            if fix_yaml_file(file_path, args.dry_run):
                fixed_count += 1
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            import traceback
            traceback.print_exc()
    
    print(f"\nProcessing complete:")
    print(f"  Files processed: {len(yaml_files)}")
    print(f"  Files {'would be ' if args.dry_run else ''}fixed: {fixed_count}")
    
    return 0


if __name__ == '__main__':
    exit(main())