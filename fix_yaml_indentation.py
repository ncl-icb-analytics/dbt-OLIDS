#!/usr/bin/env python3
"""
Fix YAML indentation issues in test configurations.
Specifically handles cases where list items under combination_of_columns are misaligned.
"""

import os
import re


def fix_yaml_indentation_issues(content: str) -> str:
    """Fix common YAML indentation issues in test configurations."""
    # Pattern to match the problematic indentation in combination_of_columns
    # This catches cases where list items are not properly indented under the parent key
    patterns_to_fix = [
        # Pattern 1: combination_of_columns with misaligned list items
        (r'(\s+)(combination_of_columns:\s*\n)(\s+- \w+\n)([\s\S]*?)(?=\n\s+- \w+:|\n\s+arguments:|\n\s+config:|\Z)',
         lambda m: fix_combination_of_columns_indentation(m)),
        
        # Pattern 2: Other list arguments with similar issues
        (r'(\s+)(\w+:\s*\n)(\s+- [^\n]+\n)(\s*)(-[^\n]+)',
         lambda m: fix_generic_list_indentation(m))
    ]
    
    fixed_content = content
    
    for pattern, fix_func in patterns_to_fix:
        fixed_content = re.sub(pattern, fix_func, fixed_content, flags=re.MULTILINE)
    
    return fixed_content


def fix_combination_of_columns_indentation(match):
    """Fix indentation for combination_of_columns blocks."""
    base_indent = match.group(1)
    key_line = match.group(2)
    first_item = match.group(3)
    rest_content = match.group(4) if len(match.groups()) > 3 else ""
    
    # Calculate proper indentation
    key_indent_level = len(base_indent)
    list_indent = ' ' * (key_indent_level + 2)  # 2 more spaces for list items
    
    # Fix the first item
    fixed_first = list_indent + first_item.lstrip()
    
    # Fix any additional items in the rest_content that are part of this list
    lines = rest_content.split('\n')
    fixed_lines = []
    
    for line in lines:
        if line.strip().startswith('- ') and not line.strip().startswith('- name:'):
            # This is likely a continuation of the list
            fixed_lines.append(list_indent + line.lstrip())
        else:
            fixed_lines.append(line)
    
    return base_indent + key_line + fixed_first + '\n'.join(fixed_lines)


def fix_generic_list_indentation(match):
    """Fix generic list indentation issues."""
    base_indent = match.group(1)
    key_line = match.group(2)
    first_item = match.group(3)
    spacing = match.group(4)
    second_item = match.group(5)
    
    # Calculate proper indentation
    key_indent_level = len(base_indent)
    list_indent = ' ' * (key_indent_level + 2)
    
    # Fix both items
    fixed_first = list_indent + first_item.lstrip()
    fixed_second = list_indent + second_item.lstrip()
    
    return base_indent + key_line + fixed_first + fixed_second


def process_yaml_file(file_path: str, dry_run: bool = False) -> bool:
    """Process a single YAML file to fix indentation issues."""
    print(f"Processing: {file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        print(f"  Error reading: {e}")
        return False
    
    # Check for common indentation problems
    has_issues = (
        re.search(r'\n\s+combination_of_columns:\s*\n\s+- \w+\n\s*-', content) or
        re.search(r'\n\s+\w+:\s*\n\s+- [^\n]+\n\s*-[^\n]+', content)
    )
    
    if not has_issues:
        print("  No indentation issues found")
        return False
    
    print("  Found potential indentation issues")
    
    if dry_run:
        print("  Would fix (dry run)")
        return True
    
    # Apply fixes
    fixed_content = fix_yaml_indentation_issues(content)
    
    if fixed_content != content:
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(fixed_content)
            print("  Fixed")
            return True
        except Exception as e:
            print(f"  Error writing: {e}")
            return False
    
    print("  No changes made")
    return False


def find_yaml_files(directory: str = 'models') -> list:
    """Find all YAML files."""
    yaml_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(('.yml', '.yaml')):
                yaml_files.append(os.path.join(root, file))
    return yaml_files


def main():
    yaml_files = find_yaml_files()
    print(f"Found {len(yaml_files)} YAML files")
    
    # Apply fixes directly
    fixed_count = 0
    for file_path in yaml_files:
        try:
            if process_yaml_file(file_path, dry_run=False):
                fixed_count += 1
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
    
    print(f"\nFixed {fixed_count} files")


if __name__ == '__main__':
    main()