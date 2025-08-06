#!/usr/bin/env python3
"""
Fix duplicate arguments keys and malformed YAML structure.
"""

import os
import re


def fix_duplicate_arguments(content: str) -> str:
    """Fix duplicate arguments keys and malformed test structures."""
    
    # Pattern to match the malformed structure:
    # dbt_utils.unique_combination_of_columns with embedded other test
    pattern = r'''
        (\s+- dbt_utils\.unique_combination_of_columns:\s*\n)  # Test start
        (\s+arguments:\s*\n)                                  # First arguments
        (\s+combination_of_columns:\s*\n)                     # combination_of_columns
        ((?:\s+- [^\n]+\n)*)                                  # List items
        (\s+- \w+:\s*\n)                                      # Embedded test (like bnf_codes_exist)
        (\s+arguments:\s*\n)                                  # Second arguments (duplicate!)
        ((?:\s+[^\n]+:[^\n]*\n)*)                             # Test parameters
    '''
    
    def fix_structure(match):
        test_start = match.group(1)
        first_args = match.group(2)
        combo_key = match.group(3)
        list_items = match.group(4)
        embedded_test = match.group(5)
        second_args = match.group(6)
        test_params = match.group(7)
        
        # Calculate proper indentation
        base_indent = len(test_start) - len(test_start.lstrip())
        test_indent = ' ' * base_indent
        args_indent = ' ' * (base_indent + 2)
        
        # Reconstruct properly
        fixed = (test_start +
                first_args +
                combo_key +
                list_items +
                test_indent + embedded_test.lstrip() +
                args_indent + second_args.lstrip() +
                test_params)
        
        return fixed
    
    # Apply the fix
    fixed_content = re.sub(pattern, fix_structure, content, flags=re.VERBOSE)
    return fixed_content


def process_file(file_path: str) -> bool:
    """Process a single file to fix duplicate arguments."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Check if file has duplicate arguments issue
        if 'arguments:' not in content:
            return False
            
        # Count arguments occurrences per test
        lines = content.split('\n')
        has_duplicate = False
        
        for i, line in enumerate(lines):
            if 'dbt_utils.unique_combination_of_columns:' in line:
                # Look for duplicate arguments in the next 20 lines
                args_count = 0
                for j in range(i+1, min(i+20, len(lines))):
                    if lines[j].strip() == 'arguments:':
                        args_count += 1
                    elif lines[j].strip().startswith('- ') and not lines[j].strip().startswith('  '):
                        break  # Next test started
                
                if args_count > 1:
                    has_duplicate = True
                    break
        
        if not has_duplicate:
            return False
        
        print(f"Fixing: {file_path}")
        
        # Manual fix approach - split and reconstruct
        fixed_content = manual_fix_yaml(content)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(fixed_content)
        
        return True
        
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False


def manual_fix_yaml(content: str) -> str:
    """Manually fix the YAML structure by parsing and reconstructing."""
    lines = content.split('\n')
    fixed_lines = []
    i = 0
    
    while i < len(lines):
        line = lines[i]
        
        # Look for the problematic pattern
        if 'dbt_utils.unique_combination_of_columns:' in line:
            # Start collecting this test block
            test_lines = [line]
            base_indent = len(line) - len(line.lstrip())
            i += 1
            
            # Collect the complete test structure
            combination_lines = []
            other_test_lines = []
            in_combination = False
            found_embedded_test = False
            
            while i < len(lines):
                current_line = lines[i]
                
                # Stop if we hit another test at the same level
                if (current_line.strip().startswith('- ') and 
                    len(current_line) - len(current_line.lstrip()) <= base_indent and
                    ':' in current_line):
                    break
                
                # Track if we're in combination_of_columns section
                if 'combination_of_columns:' in current_line:
                    in_combination = True
                    combination_lines.append(current_line)
                elif in_combination and current_line.strip().startswith('- '):
                    combination_lines.append(current_line)
                elif current_line.strip().startswith('- ') and ':' in current_line:
                    # This is an embedded test (like bnf_codes_exist)
                    found_embedded_test = True
                    in_combination = False
                    other_test_lines.append(' ' * base_indent + current_line.lstrip())  # Fix indentation
                elif 'arguments:' in current_line and found_embedded_test:
                    # This is the second arguments (for the embedded test)
                    other_test_lines.append(' ' * (base_indent + 2) + current_line.lstrip())
                elif found_embedded_test:
                    # Parameters for the embedded test
                    other_test_lines.append(' ' * (base_indent + 4) + current_line.lstrip())
                else:
                    if not found_embedded_test:
                        combination_lines.append(current_line)
                
                i += 1
            
            # Reconstruct the test block properly
            fixed_lines.extend([line])  # Test name
            fixed_lines.extend([' ' * (base_indent + 2) + 'arguments:'])  # Arguments
            fixed_lines.extend(combination_lines[1:])  # Skip the combination_of_columns line we already have
            fixed_lines.extend(combination_lines[:1])   # Add combination_of_columns line
            
            # Add the other test as a separate test
            if other_test_lines:
                fixed_lines.extend(other_test_lines)
            
            continue
        
        fixed_lines.append(line)
        i += 1
    
    return '\n'.join(fixed_lines)


def main():
    # List of files that likely have this issue
    medication_files = [
        'models/intermediate/medications/int_antidepressant_medications_all.yml',
        'models/intermediate/medications/int_antiplatelet_medications_all.yml', 
        'models/intermediate/medications/int_arb_medications_all.yml',
        'models/intermediate/medications/int_beta_blocker_medications_all.yml',
        'models/intermediate/medications/int_cardiac_glycoside_medications_all.yml',
        'models/intermediate/medications/int_diabetes_medications_all.yml',
        'models/intermediate/medications/int_diuretic_medications_all.yml',
        'models/intermediate/medications/int_inhaled_corticosteroid_medications_all.yml',
        'models/intermediate/medications/int_lithium_medications_all.yml',
        'models/intermediate/medications/int_nsaid_medications_all.yml',
        'models/intermediate/medications/int_ppi_medications_all.yml',
        'models/intermediate/medications/int_statin_medications_all.yml',
        'models/intermediate/medications/int_systemic_corticosteroid_medications_all.yml'
    ]
    
    fixed_count = 0
    for file_path in medication_files:
        if os.path.exists(file_path):
            if process_file(file_path):
                fixed_count += 1
    
    print(f"Fixed {fixed_count} files")


if __name__ == '__main__':
    main()