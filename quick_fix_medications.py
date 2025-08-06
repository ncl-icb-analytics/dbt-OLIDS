#!/usr/bin/env python3
"""
Quick fix for medication YAML files with duplicate arguments.
"""

import os
import re

# Files that need fixing based on the error messages
files_to_fix = [
    'models/intermediate/medications/int_diabetes_medications_all.yml',
    'models/intermediate/medications/int_antiplatelet_medications_all.yml', 
    'models/intermediate/medications/int_arb_medications_all.yml',
    'models/intermediate/medications/int_beta_blocker_medications_all.yml',
    'models/intermediate/medications/int_cardiac_glycoside_medications_all.yml',
    'models/intermediate/medications/int_diuretic_medications_all.yml',
    'models/intermediate/medications/int_inhaled_corticosteroid_medications_all.yml',
    'models/intermediate/medications/int_lithium_medications_all.yml',
    'models/intermediate/medications/int_nsaid_medications_all.yml',
    'models/intermediate/medications/int_ppi_medications_all.yml',
    'models/intermediate/medications/int_statin_medications_all.yml',
    'models/intermediate/medications/int_systemic_corticosteroid_medications_all.yml'
]

def fix_file(file_path):
    """Fix a single file."""
    if not os.path.exists(file_path):
        return False
        
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Pattern to match the broken structure
    # This pattern looks for the malformed combination where bnf_codes_exist is embedded
    pattern = r'''
        (\s+- dbt_utils\.unique_combination_of_columns:\s*\n)
        (\s+arguments:\s*\n)
        (\s+combination_of_columns:\s*\n)
        ((?:\s+- [^\n]+\n)+)
        (\s+- bnf_codes_exist:\s*\n)
        (\s+arguments:\s*\n)
        (\s+bnf_codes:[^\n]+\n)
    '''
    
    # Find and fix
    match = re.search(pattern, content, re.VERBOSE)
    if match:
        test_name = match.group(1)
        first_args = match.group(2)
        combo_key = match.group(3)
        list_items = match.group(4)
        bnf_test = match.group(5)
        second_args = match.group(6)
        bnf_param = match.group(7)
        
        # Calculate indentation
        test_indent = len(test_name) - len(test_name.lstrip())
        args_indent = ' ' * (test_indent + 2)
        param_indent = ' ' * (test_indent + 4)
        
        # Reconstruct properly
        fixed_block = (
            test_name +
            args_indent + 'arguments:\n' +
            param_indent + 'combination_of_columns:\n' +
            list_items.replace('  - ', '      - ') +  # Fix list item indentation
            ' ' * test_indent + '- bnf_codes_exist:\n' +
            args_indent + 'arguments:\n' +
            param_indent + bnf_param.lstrip()
        )
        
        # Replace in content
        content = content.replace(match.group(0), fixed_block)
        
        # Write back
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"Fixed: {file_path}")
        return True
    
    return False

# Fix all files
fixed_count = 0
for file_path in files_to_fix:
    if fix_file(file_path):
        fixed_count += 1

print(f"Fixed {fixed_count} files")