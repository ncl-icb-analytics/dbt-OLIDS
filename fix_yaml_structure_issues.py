#!/usr/bin/env python3
"""
Targeted YAML structure fixer for files damaged by the comprehensive script.
Fixes specific structural issues that broke the YAML syntax.
"""

import os
import re
from pathlib import Path

def fix_yaml_structure(file_path: str) -> bool:
    """
    Fix specific YAML structural issues in the file.
    
    Args:
        file_path: Path to the YAML file to fix
        
    Returns:
        bool: True if changes were made, False otherwise
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        changes_made = []
        
        # Fix 1: Restore proper test indentation under columns
        # Pattern: tests that lost their proper indentation
        content = re.sub(
            r'(\s+- name: [^\n]+\s*\n(?:\s+description: [^\n]*\n)*(?:\s+data_type: [^\n]*\n)*\s*\n)\s*(tests:\s*\n)(- )',
            r'\1    \2      \3',
            content,
            flags=re.MULTILINE
        )
        
        # Fix 2: Fix tests that are directly under column names without proper indentation
        content = re.sub(
            r'(\s+- name: [^\n]+\s*\n(?:\s+description: [^\n]*\n)*(?:\s+data_type: [^\n]*\n)*)\n(- )',
            r'\1\n\n    tests:\n      \2',
            content,
            flags=re.MULTILINE
        )
        
        # Fix 3: Fix test items that lost indentation
        lines = content.split('\n')
        fixed_lines = []
        i = 0
        
        while i < len(lines):
            line = lines[i]
            
            # If we find a 'tests:' line, ensure the following test items are properly indented
            if 'tests:' in line and ':' in line:
                fixed_lines.append(line)
                test_indent = len(line) - len(line.lstrip())
                expected_test_item_indent = test_indent + 2
                
                # Look ahead for test items
                j = i + 1
                while j < len(lines):
                    next_line = lines[j]
                    if next_line.strip() == '':
                        fixed_lines.append(next_line)
                    elif next_line.strip().startswith('- '):
                        # This is a test item, ensure proper indentation
                        current_indent = len(next_line) - len(next_line.lstrip())
                        if current_indent < expected_test_item_indent:
                            fixed_lines.append(' ' * expected_test_item_indent + next_line.strip())
                            changes_made.append(f"Fixed test item indentation at line {j+1}")
                        else:
                            fixed_lines.append(next_line)
                    elif next_line.strip().startswith('- name:'):
                        # We've reached the next column, stop processing test items
                        break
                    elif ':' in next_line and not next_line.strip().startswith('-'):
                        # This might be test parameters, ensure proper indentation
                        current_indent = len(next_line) - len(next_line.lstrip())
                        if current_indent < expected_test_item_indent + 2:
                            fixed_lines.append(' ' * (expected_test_item_indent + 2) + next_line.strip())
                            changes_made.append(f"Fixed test parameter indentation at line {j+1}")
                        else:
                            fixed_lines.append(next_line)
                    else:
                        fixed_lines.append(next_line)
                    j += 1
                
                i = j - 1
            
            # If we find a column definition followed directly by a test (no 'tests:' line)
            elif (re.match(r'\s+- name: ', line) and 
                  i + 3 < len(lines) and 
                  lines[i + 3].strip().startswith('- ') and
                  'tests:' not in lines[i + 2]):
                
                # Add the column line
                fixed_lines.append(line)
                
                # Add description/data_type lines if they exist
                j = i + 1
                while j < len(lines) and not lines[j].strip().startswith('- '):
                    if lines[j].strip() and not lines[j].strip().startswith('#'):
                        fixed_lines.append(lines[j])
                    j += 1
                
                # Add a proper 'tests:' line
                column_indent = len(line) - len(line.lstrip())
                fixed_lines.append('')
                fixed_lines.append(' ' * (column_indent + 2) + 'tests:')
                changes_made.append(f"Added missing 'tests:' line at column on line {i+1}")
                
                i = j - 1
            else:
                fixed_lines.append(line)
            
            i += 1
        
        content = '\n'.join(fixed_lines)
        
        # Fix 4: Remove duplicate empty lines
        content = re.sub(r'\n\s*\n\s*\n+', '\n\n', content)
        
        # Fix 5: Ensure file ends with newline
        if not content.endswith('\n'):
            content += '\n'
        
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"[FIXED] {file_path}")
            for change in changes_made:
                print(f"   - {change}")
            return True
        
        return False
        
    except Exception as e:
        print(f"[ERROR] Error processing {file_path}: {str(e)}")
        return False

def main():
    """Main function to process files with known structural issues."""
    
    # Priority files that showed structural errors in the hook output
    priority_files = [
        "models/marts/organisation/dim_pcn.yml",
        "models/staging/flu_staging_schema.yml", 
        "models/marts/programme/ltc_lcs/cf/fct_ltc_lcs_person_dashboard.yml",
        "models/marts/organisation/dim_practice_neighbourhood.yml",
        "models/marts/programme/ltc_lcs/cf/fct_ltc_lcs_population_summary.yml",
        "models/marts/organisation/dim_person_historical_practice.yml",
        "models/marts/programme/flu/flu_marts_schema.yml",
        # Also files I know were corrupted by the previous script
        "models/marts/disease_registers/qof/fct_person_copd_register.yml",
        "models/intermediate/medications/int_systemic_corticosteroid_medications_all.yml",
        "models/intermediate/medications/int_statin_medications_all.yml",
        "models/intermediate/medications/int_ppi_medications_all.yml",
        "models/intermediate/medications/int_nsaid_medications_all.yml"
    ]
    
    print(f"[STRUCTURE] Fixing YAML structural issues in {len(priority_files)} priority files...")
    fixed_count = 0
    
    for file_path in priority_files:
        if os.path.exists(file_path):
            if fix_yaml_structure(file_path):
                fixed_count += 1
        else:
            print(f"[SKIP] File not found: {file_path}")
    
    print(f"\n[DONE] Fixed structural issues in {fixed_count} files")

if __name__ == "__main__":
    main()