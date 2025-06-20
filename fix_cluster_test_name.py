#!/usr/bin/env python3
"""
Fix cluster_ids_exist test syntax in YAML files.

This script replaces instances of `- test_cluster_ids_exist:` with `- cluster_ids_exist:`
to use the correct generic test syntax.
"""

import os
import re
import argparse

def fix_cluster_test_in_file(file_path):
    """Fix cluster_ids_exist test syntax in a single YAML file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Pattern to match `- test_cluster_ids_exist:` (with any amount of leading whitespace)
        pattern = r'^(\s*)- test_cluster_ids_exist:'
        replacement = r'\1- cluster_ids_exist:'
        
        # Count matches before replacement
        matches = re.findall(pattern, content, re.MULTILINE)
        if not matches:
            return 0
        
        # Apply the replacement
        new_content = re.sub(pattern, replacement, content, flags=re.MULTILINE)
        
        if new_content != content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
            return len(matches)
        
        return 0
        
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return 0

def find_yaml_files(directory):
    """Find all YAML files in the given directory."""
    yaml_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.yml') or file.endswith('.yaml'):
                yaml_files.append(os.path.join(root, file))
    return yaml_files

def main():
    parser = argparse.ArgumentParser(description='Fix cluster_ids_exist test syntax in YAML files')
    parser.add_argument('directory', nargs='?', default='models', 
                       help='Directory to search for YAML files (default: models)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be changed without making changes')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.directory):
        print(f"Error: Directory '{args.directory}' does not exist")
        return 1
    
    yaml_files = find_yaml_files(args.directory)
    print(f"Found {len(yaml_files)} YAML files to check...")
    
    total_fixes = 0
    files_fixed = 0
    
    for file_path in yaml_files:
        if args.dry_run:
            # For dry run, just check for matches
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                pattern = r'^(\s*)- test_cluster_ids_exist:'
                matches = re.findall(pattern, content, re.MULTILINE)
                
                if matches:
                    rel_path = os.path.relpath(file_path)
                    print(f"Would fix {len(matches)} instances in: {rel_path}")
                    total_fixes += len(matches)
                    files_fixed += 1
                    
            except Exception as e:
                print(f"Error reading {file_path}: {e}")
        else:
            # Actually fix the file
            fixes_made = fix_cluster_test_in_file(file_path)
            if fixes_made > 0:
                rel_path = os.path.relpath(file_path)
                print(f"Fixed {fixes_made} instances in: {rel_path}")
                total_fixes += fixes_made
                files_fixed += 1
    
    action = "Would fix" if args.dry_run else "Fixed"
    print(f"\n{action} {total_fixes} test_cluster_ids_exist instances across {files_fixed} files")
    
    if args.dry_run:
        print("\nRun without --dry-run to apply the changes")
    else:
        print("\nAll fixes applied successfully!")
    
    return 0

if __name__ == '__main__':
    exit(main()) 