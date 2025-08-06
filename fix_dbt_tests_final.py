#!/usr/bin/env python3
"""
Fix dbt test format deprecation warnings - line by line approach.
Moves model-level tests from before description to after description.
"""

import os
import argparse


def find_yaml_files(directory: str) -> list[str]:
    """Find all YAML files in the models directory."""
    yaml_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(('.yml', '.yaml')):
                yaml_files.append(os.path.join(root, file))
    return yaml_files


def fix_yaml_file(file_path: str, dry_run: bool = False) -> bool:
    """Fix deprecated test format by reordering lines."""
    print(f"Processing: {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    modified = False
    new_lines = []
    i = 0
    
    while i < len(lines):
        line = lines[i]
        
        # Look for model definition
        if line.strip().startswith('- name:') and ':' not in line.split('- name:')[1].strip():
            model_start = i
            new_lines.append(line)
            i += 1
            
            # Collect lines until we find tests or description
            model_lines = []
            tests_lines = []
            description_lines = []
            remaining_lines = []
            
            current_section = 'model'
            base_indent = len(line) - len(line.lstrip())
            
            while i < len(lines):
                current_line = lines[i]
                
                # Check if we've moved to next model or end
                if (current_line.strip() and 
                    len(current_line) - len(current_line.lstrip()) <= base_indent and
                    current_line.strip().startswith('- name:')):
                    break
                
                # Detect section changes
                if current_line.strip().startswith('tests:'):
                    current_section = 'tests'
                    tests_lines.append(current_line)
                elif current_line.strip().startswith('description:'):
                    current_section = 'description'
                    description_lines.append(current_line)
                elif current_line.strip().startswith('columns:'):
                    current_section = 'remaining'
                    remaining_lines.append(current_line)
                elif current_section == 'tests':
                    tests_lines.append(current_line)
                elif current_section == 'description':
                    description_lines.append(current_line)
                elif current_section == 'remaining':
                    remaining_lines.append(current_line)
                else:
                    model_lines.append(current_line)
                
                i += 1
            
            # Check if we need to reorder (tests before description)
            if tests_lines and description_lines:
                print("  Found deprecated test format - fixing...")
                # Add in correct order: model metadata, description, tests, remaining
                new_lines.extend(model_lines)
                new_lines.extend(description_lines)
                new_lines.extend(tests_lines)
                new_lines.extend(remaining_lines)
                modified = True
            else:
                # No reordering needed
                new_lines.extend(model_lines)
                new_lines.extend(tests_lines)
                new_lines.extend(description_lines)
                new_lines.extend(remaining_lines)
            
            continue
        
        new_lines.append(line)
        i += 1
    
    if not modified:
        print("  No deprecated format found")
        return False
    
    if not dry_run:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.writelines(new_lines)
        print("  Fixed")
    else:
        print("  Would fix (dry run)")
    
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