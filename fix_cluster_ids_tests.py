#!/usr/bin/env python3
"""
Fix cluster_ids_exist test format to use arguments property.
This addresses the MissingArgumentsPropertyInGenericTestDeprecation.
"""

import os
import re


def fix_cluster_ids_test(file_path: str, dry_run: bool = False) -> bool:
    """Fix cluster_ids_exist test format in a single file."""
    print(f"Processing: {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Pattern to match cluster_ids_exist tests with direct cluster_ids parameter
    pattern = r'(\s+- cluster_ids_exist:\s*\n)(\s+)(cluster_ids:\s*[^\n]+(?:\n\s+[^\n-]*)*)'
    
    matches = list(re.finditer(pattern, content))
    
    if not matches:
        print("  No cluster_ids_exist tests found")
        return False
    
    print(f"  Found {len(matches)} cluster_ids_exist test(s) to fix")
    
    if dry_run:
        print("  Would fix (dry run)")
        return True
    
    # Replace each match
    fixed_content = content
    for match in reversed(matches):  # Reverse to avoid offset issues
        test_start = match.group(1)      # "  - cluster_ids_exist:\n"
        base_indent = match.group(2)     # Base indentation
        cluster_ids_line = match.group(3)  # "cluster_ids: ..."
        
        # Create the fixed version with arguments
        fixed_version = (test_start + 
                        base_indent + "arguments:\n" + 
                        base_indent + "  " + cluster_ids_line)
        
        fixed_content = fixed_content.replace(match.group(0), fixed_version, 1)
    
    # Write the fixed content
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(fixed_content)
    
    print("  Fixed")
    return True


def main():
    # Find all YAML files with cluster_ids_exist tests
    yaml_files = []
    for root, dirs, files in os.walk('models'):
        for file in files:
            if file.endswith(('.yml', '.yaml')):
                file_path = os.path.join(root, file)
                # Quick check if file contains cluster_ids_exist
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        if 'cluster_ids_exist:' in f.read():
                            yaml_files.append(file_path)
                except:
                    continue
    
    print(f"Found {len(yaml_files)} YAML files with cluster_ids_exist tests")
    
    # First, run a dry run to show what would be fixed
    print("\n=== DRY RUN ===")
    dry_run_count = 0
    for file_path in yaml_files:
        try:
            if fix_cluster_ids_test(file_path, dry_run=True):
                dry_run_count += 1
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
    
    print(f"\nDry run complete: {dry_run_count} files would be fixed")
    
    # Apply fixes
    print("\n=== APPLYING FIXES ===")
    fixed_count = 0
    for file_path in yaml_files:
        try:
            if fix_cluster_ids_test(file_path, dry_run=False):
                fixed_count += 1
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
    
    print(f"\nFix complete: {fixed_count} files fixed")


if __name__ == '__main__':
    main()