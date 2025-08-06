#!/usr/bin/env python3
"""
Quick fix for name parameter formatting issues after the arguments wrapper fix.
"""

import os
import re
from pathlib import Path

def fix_name_parameter_formatting(file_path: str) -> bool:
    """Fix name parameter formatting where it got concatenated to combination_of_columns."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Pattern: combination_of_columns list ending with "        name: test_name"
        # Should be: combination_of_columns list + newline + "        name: test_name"
        pattern = r'(\s+- [^\n]+)(\s+)name: ([^\n]+)'
        
        def fix_name_param(match):
            last_item = match.group(1)
            space_before_name = match.group(2)
            name_value = match.group(3)
            
            # Extract the indentation from the space before name
            # The name should be at the same level as combination_of_columns
            base_indent = len(space_before_name) - 8  # Assuming 8 spaces before combination_of_columns
            name_indent = ' ' * (base_indent + 8)
            
            return f"{last_item}\n{name_indent}name: {name_value}"
        
        content = re.sub(pattern, fix_name_param, content)
        
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"Fixed name parameter formatting in {file_path}")
            return True
        else:
            return False
            
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def main():
    """Process all YAML files in the project."""
    root_dir = Path('.')
    yaml_files = list(root_dir.glob('**/*.yml'))
    
    print(f"Found {len(yaml_files)} YAML files to process...")
    
    files_modified = 0
    
    for yaml_file in yaml_files:
        if fix_name_parameter_formatting(str(yaml_file)):
            files_modified += 1
    
    print(f"\nCompleted! Modified {files_modified} files.")

if __name__ == "__main__":
    main()