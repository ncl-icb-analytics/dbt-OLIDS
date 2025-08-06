#!/usr/bin/env python3
"""
Careful script to fix dbt test deprecation warnings by adding 'arguments:' wrapper
while preserving exact existing indentation and YAML structure.
"""

import re
import os
from pathlib import Path

def fix_test_arguments(file_path: str, dry_run: bool = False) -> bool:
    """
    Fix test deprecation warnings by adding 'arguments:' wrapper.
    Preserves exact existing indentation.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        changes_made = False
        
        # Pattern 1: cluster_ids_exist test
        pattern1 = r'(\s+)(- cluster_ids_exist:\s*\n)(\s+)(cluster_ids:\s*[^\n]+)'
        def replace1(match):
            base_indent = match.group(1)
            test_line = match.group(2)
            param_indent = match.group(3) 
            param_line = match.group(4)
            
            # Add arguments: with same indentation as the parameter was
            arguments_indent = param_indent
            new_param_indent = param_indent + "  "  # Add 2 more spaces for nesting
            
            return f"{base_indent}{test_line}{arguments_indent}arguments:\n{new_param_indent}{param_line}"
        
        content = re.sub(pattern1, replace1, content)
        if content != original_content:
            changes_made = True
            print(f"  Fixed cluster_ids_exist pattern")
        
        # Pattern 2: bnf_codes_exist test  
        pattern2 = r'(\s+)(- bnf_codes_exist:\s*\n)(\s+)(bnf_codes:\s*[^\n]+)'
        def replace2(match):
            base_indent = match.group(1)
            test_line = match.group(2)
            param_indent = match.group(3)
            param_line = match.group(4)
            
            arguments_indent = param_indent
            new_param_indent = param_indent + "  "
            
            return f"{base_indent}{test_line}{arguments_indent}arguments:\n{new_param_indent}{param_line}"
        
        content = re.sub(pattern2, replace2, content)
        if content != original_content and not changes_made:
            changes_made = True
            print(f"  Fixed bnf_codes_exist pattern")
        
        # Pattern 3: dbt_utils.accepted_range test
        pattern3 = r'(\s+)(- dbt_utils\.accepted_range:\s*\n)((?:\s+(?:min_value|max_value|severity|where|config):[^\n]*\n)*)'
        def replace3(match):
            base_indent = match.group(1)
            test_line = match.group(2)
            params_block = match.group(3)
            
            if not params_block.strip():
                return match.group(0)  # No parameters to wrap
            
            # Find the indentation of the first parameter
            first_param_match = re.search(r'^(\s+)', params_block, re.MULTILINE)
            if not first_param_match:
                return match.group(0)
                
            param_indent = first_param_match.group(1)
            arguments_indent = param_indent
            new_param_indent = param_indent + "  "
            
            # Re-indent all parameters
            new_params = re.sub(r'^' + param_indent, new_param_indent, params_block, flags=re.MULTILINE)
            
            return f"{base_indent}{test_line}{arguments_indent}arguments:\n{new_params}"
        
        content = re.sub(pattern3, replace3, content)
        if content != original_content and not changes_made:
            changes_made = True
            print(f"  Fixed dbt_utils.accepted_range pattern")
        
        # Pattern 4: dbt_utils.unique_combination_of_columns test
        pattern4 = r'(\s+)(- dbt_utils\.unique_combination_of_columns:\s*\n)((?:\s+combination_of_columns:[^\n]*(?:\n\s+- [^\n]*)*\n?)*)'
        def replace4(match):
            base_indent = match.group(1)
            test_line = match.group(2)
            params_block = match.group(3)
            
            if not params_block.strip():
                return match.group(0)
                
            first_param_match = re.search(r'^(\s+)', params_block, re.MULTILINE)
            if not first_param_match:
                return match.group(0)
                
            param_indent = first_param_match.group(1)
            arguments_indent = param_indent
            new_param_indent = param_indent + "  "
            
            new_params = re.sub(r'^' + param_indent, new_param_indent, params_block, flags=re.MULTILINE)
            
            return f"{base_indent}{test_line}{arguments_indent}arguments:\n{new_params}"
        
        content = re.sub(pattern4, replace4, content)
        if content != original_content and not changes_made:
            changes_made = True
            print(f"  Fixed dbt_utils.unique_combination_of_columns pattern")
        
        # Pattern 5: accepted_values test (non-dbt_utils)
        pattern5 = r'(\s+)(- accepted_values:\s*\n)((?:\s+values:[^\n]*(?:\n\s+- [^\n]*)*\n?)*)'
        def replace5(match):
            base_indent = match.group(1)
            test_line = match.group(2)
            params_block = match.group(3)
            
            if not params_block.strip():
                return match.group(0)
                
            first_param_match = re.search(r'^(\s+)', params_block, re.MULTILINE)
            if not first_param_match:
                return match.group(0)
                
            param_indent = first_param_match.group(1)
            arguments_indent = param_indent  
            new_param_indent = param_indent + "  "
            
            new_params = re.sub(r'^' + param_indent, new_param_indent, params_block, flags=re.MULTILINE)
            
            return f"{base_indent}{test_line}{arguments_indent}arguments:\n{new_params}"
        
        content = re.sub(pattern5, replace5, content)
        if content != original_content and not changes_made:
            changes_made = True
            print(f"  Fixed accepted_values pattern")
        
        # Write the file if changes were made
        if changes_made and not dry_run:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            return True
        elif changes_made:
            print(f"  [DRY RUN] Would make changes")
            return True
        else:
            return False
    
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def main():
    """Process all .yml files in the models directory."""
    models_dir = Path("models")
    
    if not models_dir.exists():
        print("Models directory not found")
        return
    
    yml_files = list(models_dir.rglob("*.yml"))
    print(f"Found {len(yml_files)} YAML files")
    
    fixed_count = 0
    for yml_file in yml_files:
        print(f"\nProcessing: {yml_file}")
        if fix_test_arguments(str(yml_file), dry_run=False):
            fixed_count += 1
    
    print(f"\nCompleted processing {len(yml_files)} files")
    print(f"Fixed {fixed_count} files")

if __name__ == "__main__":
    main()