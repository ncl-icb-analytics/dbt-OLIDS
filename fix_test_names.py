#!/usr/bin/env python3
"""
Fix generic test naming syntax in YAML files.

This script removes incorrect 'test_' prefixes from custom generic tests.
For example: '- test_cluster_ids_exist:' becomes '- cluster_ids_exist:'
"""

import os
import re
import argparse

# Define the test mappings (incorrect -> correct)
TEST_MAPPINGS = {
    'test_cluster_ids_exist': 'cluster_ids_exist',
    'test_bnf_codes_exist': 'bnf_codes_exist',
    'test_no_future_dates': 'no_future_dates',
    'test_all_source_columns_in_staging': 'all_source_columns_in_staging',
    'test_staging_columns_match_source': 'staging_columns_match_source',
}

def fix_test_names_in_file(file_path, test_mappings, dry_run=False):
    """Fix test naming syntax in a single YAML file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        total_fixes = 0
        fixes_by_test = {}
        
        for incorrect_name, correct_name in test_mappings.items():
            # Pattern to match `- test_name:` (with any amount of leading whitespace)
            pattern = rf'^(\s*)- {re.escape(incorrect_name)}:'
            replacement = rf'\1- {correct_name}:'
            
            # Count matches before replacement
            matches = re.findall(pattern, content, re.MULTILINE)
            if matches:
                fixes_by_test[incorrect_name] = len(matches)
                total_fixes += len(matches)
                
                if not dry_run:
                    # Apply the replacement
                    content = re.sub(pattern, replacement, content, flags=re.MULTILINE)
        
        if total_fixes > 0:
            if not dry_run and content != original_content:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
            
            return total_fixes, fixes_by_test
        
        return 0, {}
        
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return 0, {}

def find_yaml_files(directory):
    """Find all YAML files in the given directory."""
    yaml_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.yml') or file.endswith('.yaml'):
                yaml_files.append(os.path.join(root, file))
    return yaml_files

def find_custom_tests(macros_dir='macros/testing/generic'):
    """Automatically discover custom generic tests from macro files."""
    custom_tests = {}
    
    if not os.path.exists(macros_dir):
        print(f"Warning: Macros directory '{macros_dir}' not found")
        return custom_tests
    
    for root, dirs, files in os.walk(macros_dir):
        for file in files:
            if file.endswith('.sql'):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    # Look for {% test test_name(...) %} patterns
                    test_pattern = r'{%\s*test\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\('
                    matches = re.findall(test_pattern, content, re.IGNORECASE)
                    
                    for test_name in matches:
                        # If someone incorrectly calls it test_something, map it
                        if test_name.startswith('test_'):
                            # This shouldn't happen in macro definitions, but just in case
                            continue
                        else:
                            # Add mapping for the incorrect test_ prefix version
                            custom_tests[f'test_{test_name}'] = test_name
                            
                except Exception as e:
                    print(f"Error reading macro file {file_path}: {e}")
    
    return custom_tests

def main():
    parser = argparse.ArgumentParser(description='Fix generic test naming syntax in YAML files')
    parser.add_argument('directory', nargs='?', default='models', 
                       help='Directory to search for YAML files (default: models)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be changed without making changes')
    parser.add_argument('--auto-discover', action='store_true',
                       help='Automatically discover custom tests from macros directory')
    parser.add_argument('--macros-dir', default='macros/testing/generic',
                       help='Directory containing custom test macros (default: macros/testing/generic)')
    parser.add_argument('--add-mapping', action='append', nargs=2, metavar=('INCORRECT', 'CORRECT'),
                       help='Add custom test mapping: --add-mapping test_my_test my_test')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.directory):
        print(f"Error: Directory '{args.directory}' does not exist")
        return 1
    
    # Start with default mappings
    test_mappings = TEST_MAPPINGS.copy()
    
    # Auto-discover custom tests if requested
    if args.auto_discover:
        print(f"Auto-discovering custom tests from {args.macros_dir}...")
        discovered_tests = find_custom_tests(args.macros_dir)
        test_mappings.update(discovered_tests)
        if discovered_tests:
            print(f"Discovered {len(discovered_tests)} custom test mappings:")
            for incorrect, correct in discovered_tests.items():
                print(f"  {incorrect} -> {correct}")
        else:
            print("No additional custom tests discovered")
    
    # Add any custom mappings from command line
    if args.add_mapping:
        for incorrect, correct in args.add_mapping:
            test_mappings[incorrect] = correct
            print(f"Added custom mapping: {incorrect} -> {correct}")
    
    print(f"\nUsing {len(test_mappings)} test mappings:")
    for incorrect, correct in test_mappings.items():
        print(f"  {incorrect} -> {correct}")
    
    yaml_files = find_yaml_files(args.directory)
    print(f"\nFound {len(yaml_files)} YAML files to check...")
    
    total_fixes = 0
    files_fixed = 0
    all_fixes_by_test = {}
    
    for file_path in yaml_files:
        fixes_made, fixes_by_test = fix_test_names_in_file(file_path, test_mappings, args.dry_run)
        
        if fixes_made > 0:
            rel_path = os.path.relpath(file_path)
            action = "Would fix" if args.dry_run else "Fixed"
            
            details = []
            for test_name, count in fixes_by_test.items():
                details.append(f"{count}x {test_name}")
                # Track overall statistics
                if test_name not in all_fixes_by_test:
                    all_fixes_by_test[test_name] = 0
                all_fixes_by_test[test_name] += count
            
            print(f"{action} {fixes_made} instances in {rel_path}: {', '.join(details)}")
            total_fixes += fixes_made
            files_fixed += 1
    
    print(f"\n=== Summary ===")
    action = "Would fix" if args.dry_run else "Fixed"
    print(f"{action} {total_fixes} test instances across {files_fixed} files")
    
    if all_fixes_by_test:
        print("\nBreakdown by test type:")
        for test_name, count in sorted(all_fixes_by_test.items()):
            correct_name = test_mappings[test_name]
            print(f"  {test_name} -> {correct_name}: {count} instances")
    
    if args.dry_run:
        print("\nRun without --dry-run to apply the changes")
    else:
        print("\nAll fixes applied successfully!")
    
    return 0

if __name__ == '__main__':
    exit(main()) 