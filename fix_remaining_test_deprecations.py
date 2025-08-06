#!/usr/bin/env python3
"""
Fix remaining dbt test deprecation warnings for patterns not caught by initial script.
Handles: expression, column_name, quote, values (at root level)
"""

import re
import os
from pathlib import Path

def fix_remaining_test_arguments(file_path: str, dry_run: bool = False) -> bool:
    """
    Fix remaining test deprecation warnings by adding 'arguments:' wrapper.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        changes_made = False
        
        # Pattern 1: dbt_utils.expression_is_true with expression at top level
        pattern1 = r'(\s+)(- dbt_utils\.expression_is_true:\s*\n)(\s+)(expression:\s*[^\n]+(?:\n\s+[^\n-]*)*)'
        def replace1(match):
            base_indent = match.group(1)
            test_line = match.group(2)
            param_indent = match.group(3)
            expression_line = match.group(4)
            
            arguments_indent = param_indent
            new_param_indent = param_indent + "  "
            
            # Handle multi-line expressions
            expression_lines = expression_line.split('\n')
            new_expression = '\n'.join([
                new_param_indent + line.lstrip() if i == 0 
                else param_indent + "  " + line.lstrip() 
                for i, line in enumerate(expression_lines)
            ])
            
            return f"{base_indent}{test_line}{arguments_indent}arguments:\n{new_expression}"
        
        content = re.sub(pattern1, replace1, content)
        if content != original_content:
            changes_made = True
            print(f"  Fixed dbt_utils.expression_is_true pattern")
        
        # Pattern 2: Simple accepted_values with values at top level (not under arguments)
        pattern2 = r'(\s+)(- accepted_values:\s*\n)(\s+)(?!arguments:)(values:\s*(?:\n\s+- [^\n]+)+)'
        def replace2(match):
            base_indent = match.group(1)
            test_line = match.group(2)
            param_indent = match.group(3)
            values_block = match.group(4)
            
            arguments_indent = param_indent
            new_param_indent = param_indent + "  "
            
            # Re-indent the values block
            new_values = re.sub(r'^' + param_indent, new_param_indent, values_block, flags=re.MULTILINE)
            
            return f"{base_indent}{test_line}{arguments_indent}arguments:\n{new_values}"
        
        new_content = re.sub(pattern2, replace2, content)
        if new_content != content:
            changes_made = True
            content = new_content
            print(f"  Fixed accepted_values with top-level values")
        
        # Pattern 3: Tests with column_name parameter at top level
        pattern3 = r'(\s+)(- not_null:\s*\n)(\s+)(?!arguments:)(column_name:\s*[^\n]+)'
        def replace3(match):
            base_indent = match.group(1)
            test_line = match.group(2)
            param_indent = match.group(3)
            column_line = match.group(4)
            
            arguments_indent = param_indent
            new_param_indent = param_indent + "  "
            
            return f"{base_indent}{test_line}{arguments_indent}arguments:\n{new_param_indent}{column_line}"
        
        content = re.sub(pattern3, replace3, content)
        if content != original_content and not changes_made:
            changes_made = True
            print(f"  Fixed not_null with column_name")
        
        # Pattern 4: accepted_values with quote parameter
        pattern4 = r'(\s+)(- accepted_values:\s*\n)((?:\s+(?:values|quote):[^\n]*(?:\n\s+- [^\n]*)*\n?)+)'
        def replace4(match):
            base_indent = match.group(1)
            test_line = match.group(2)
            params_block = match.group(3)
            
            # Check if arguments: already exists
            if 'arguments:' in params_block:
                return match.group(0)
            
            # Check if quote is at top level
            if not re.search(r'^\s+quote:', params_block, re.MULTILINE):
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
            print(f"  Fixed accepted_values with quote parameter")
        
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
    """Process specific files with remaining deprecations."""
    
    files_to_fix = [
        "models/intermediate/diagnoses/qof/int_diabetes_diagnoses_all.yml",
        "models/intermediate/organisation/int_organisation_borough_mapping.yml", 
        "models/intermediate/programme/cervical_screening/int_cervical_screening_all.yml",
        "models/intermediate/programme/cervical_screening/int_cervical_screening_latest.yml",
        "models/marts/disease_registers/fct_person_cyp_asthma_register.yml",
        "models/marts/disease_registers/qof/fct_person_asthma_register.yml",
        "models/marts/disease_registers/qof/fct_person_atrial_fibrillation_register.yml",
        "models/marts/disease_registers/qof/fct_person_diabetes_register.yml",
        "models/marts/disease_registers/qof/fct_person_epilepsy_register.yml",
        "models/marts/disease_registers/qof/fct_person_heart_failure_register.yml",
        "models/marts/organisation/dim_pcn.yml",
        "models/marts/organisation/dim_practice_neighbourhood.yml",
        "models/marts/organisation/dim_practice.yml",
        "models/marts/programme/flu/flu_marts_schema.yml",
        "models/marts/programme/ltc_lcs/cf/fct_ltc_lcs_person_dashboard.yml",
        "models/staging/flu_staging_schema.yml"
    ]
    
    fixed_count = 0
    for file_path in files_to_fix:
        full_path = Path(file_path)
        if full_path.exists():
            print(f"\nProcessing: {file_path}")
            if fix_remaining_test_arguments(str(full_path), dry_run=False):
                fixed_count += 1
        else:
            print(f"\nFile not found: {file_path}")
    
    print(f"\nCompleted processing {len(files_to_fix)} files")
    print(f"Fixed {fixed_count} files")

if __name__ == "__main__":
    main()