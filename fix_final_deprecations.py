#!/usr/bin/env python3
"""
Final comprehensive fix for all remaining dbt test deprecation warnings.
"""

import re
import os
from pathlib import Path

def fix_all_test_patterns(file_path: str, dry_run: bool = False) -> bool:
    """
    Fix ALL test deprecation warnings comprehensively.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        changes_made = False
        
        # Pattern 1: dbt_utils.at_least_one with column_name
        pattern1 = r'(\s+)(- dbt_utils\.at_least_one:\s*\n)(\s+)(?!arguments:)(column_name:\s*[^\n]+)'
        content = re.sub(pattern1, lambda m: f"{m.group(1)}{m.group(2)}{m.group(3)}arguments:\n{m.group(3)}  {m.group(4)}", content)
        
        # Pattern 2: not_null with column_name  
        pattern2 = r'(\s+)(- not_null:\s*\n)(\s+)(?!arguments:)(column_name:\s*[^\n]+)'
        content = re.sub(pattern2, lambda m: f"{m.group(1)}{m.group(2)}{m.group(3)}arguments:\n{m.group(3)}  {m.group(4)}", content)
        
        # Pattern 3: accepted_values with quote parameter at top level
        pattern3 = r'(\s+)(- accepted_values:\s*\n)(\s+)(?!arguments:)(quote:\s*[^\n]+)'
        content = re.sub(pattern3, lambda m: f"{m.group(1)}{m.group(2)}{m.group(3)}arguments:\n{m.group(3)}  {m.group(4)}", content)
        
        # Pattern 4: accepted_values with values: at wrong indentation (line 41-43 pattern)
        # This is for values: that is not properly indented under accepted_values test
        pattern4 = r'(\s+)(- accepted_values:\s*\n)\s*\n(\s+)values:'
        content = re.sub(pattern4, lambda m: f"{m.group(1)}{m.group(2)}{m.group(3)}arguments:\n{m.group(3)}  values:", content)
        
        # Pattern 5: expression_is_true with expression at top level
        pattern5 = r'(\s+)(- dbt_utils\.expression_is_true:\s*\n)(\s+)(?!arguments:)(expression:[^\n]*(?:\n(?!\s+-).*)*)'
        def replace5(match):
            base_indent = match.group(1)
            test_line = match.group(2)
            param_indent = match.group(3)
            expression = match.group(4)
            
            # Handle multi-line expressions properly
            lines = expression.split('\n')
            new_lines = []
            for i, line in enumerate(lines):
                if i == 0:
                    new_lines.append(param_indent + "  " + line)
                elif line.strip():
                    # Preserve relative indentation for continuation lines
                    new_lines.append(param_indent + "    " + line.strip())
            
            return f"{base_indent}{test_line}{param_indent}arguments:\n" + '\n'.join(new_lines)
        
        content = re.sub(pattern5, replace5, content)
        
        if content != original_content:
            changes_made = True
            
        # Write the file if changes were made
        if changes_made and not dry_run:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"  Fixed deprecations in {Path(file_path).name}")
            return True
        elif changes_made:
            print(f"  [DRY RUN] Would fix {Path(file_path).name}")
            return True
        else:
            return False
    
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def main():
    """Process all YAML files that still have deprecation warnings."""
    
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
        "models/staging/flu_staging_schema.yml",
        "models/marts/geography/fct_household_members.yml"  # Also check this file
    ]
    
    fixed_count = 0
    for file_path in files_to_fix:
        full_path = Path(file_path)
        if full_path.exists():
            print(f"\nProcessing: {file_path}")
            if fix_all_test_patterns(str(full_path), dry_run=False):
                fixed_count += 1
        else:
            print(f"\nFile not found: {file_path}")
    
    print(f"\nCompleted processing {len(files_to_fix)} files")
    print(f"Fixed {fixed_count} files")

if __name__ == "__main__":
    main()