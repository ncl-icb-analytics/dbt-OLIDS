#!/usr/bin/env python3
"""
Fix specific YAML test indentation issues where tests are not properly indented under 'tests:' keys.
This fixes the dbt1013 error: invalid type: string "not_null", expected struct MinimalSchemaValue.
"""

import os
import re
from pathlib import Path

def fix_test_indentation(file_path: str) -> bool:
    """
    Fix test indentation issues in YAML files.
    
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
        
        lines = content.split('\n')
        fixed_lines = []
        i = 0
        
        while i < len(lines):
            line = lines[i]
            
            # Look for a pattern where we have a tests: line followed by improperly indented test items
            if re.match(r'\s+tests:\s*$', line):
                # Found a tests: line
                fixed_lines.append(line)
                tests_indent = len(line) - len(line.lstrip())
                expected_test_indent = tests_indent + 2
                
                # Look ahead for test items that should be indented
                j = i + 1
                while j < len(lines):
                    next_line = lines[j]
                    
                    # Skip empty lines
                    if not next_line.strip():
                        fixed_lines.append(next_line)
                        j += 1
                        continue
                    
                    # Check if this is a test item that's incorrectly indented
                    if re.match(r'^- (not_null|unique|relationships|accepted_values|dbt_utils\.|cluster_ids_exist|bnf_codes_exist)', next_line.strip()):
                        # This test item is at root level, needs to be indented under tests:
                        current_indent = len(next_line) - len(next_line.lstrip())
                        if current_indent < expected_test_indent:
                            # Re-indent this line properly
                            fixed_lines.append(' ' * expected_test_indent + next_line.strip())
                            changes_made.append(f"Fixed test indentation at line {j+1}: {next_line.strip()}")
                        else:
                            fixed_lines.append(next_line)
                    elif re.match(r'\s+- name:', next_line) or re.match(r'\s+tests:', next_line):
                        # We've reached the next column or another tests block
                        break
                    elif next_line.strip().startswith('to:') or next_line.strip().startswith('field:') or next_line.strip().startswith('values:'):
                        # This is test configuration, ensure proper indentation
                        current_indent = len(next_line) - len(next_line.lstrip())
                        if current_indent < expected_test_indent + 2:
                            fixed_lines.append(' ' * (expected_test_indent + 2) + next_line.strip())
                            changes_made.append(f"Fixed test config indentation at line {j+1}: {next_line.strip()}")
                        else:
                            fixed_lines.append(next_line)
                    else:
                        fixed_lines.append(next_line)
                    
                    j += 1
                
                i = j - 1
            
            # Look for root-level test items that don't have a proper tests: parent
            elif re.match(r'^- (not_null|unique|relationships|accepted_values|dbt_utils\.|cluster_ids_exist|bnf_codes_exist)', line):
                # This is a test item at root level, which is wrong
                # Look backward to see if we can find where it should belong
                
                # Search backward for the most recent column definition
                k = i - 1
                while k >= 0:
                    prev_line = lines[k]
                    if re.match(r'\s+- name:', prev_line):
                        # Found the column this test should belong to
                        column_indent = len(prev_line) - len(prev_line.lstrip())
                        
                        # Check if there's already a tests: line
                        tests_line_exists = False
                        m = k + 1
                        while m < i:
                            if re.match(r'\s+tests:\s*$', lines[m]):
                                tests_line_exists = True
                                break
                            m += 1
                        
                        if not tests_line_exists:
                            # Need to add a tests: line
                            # First, add any description/data_type lines that come before the test
                            temp_lines = []
                            for n in range(k + 1, i):
                                if lines[n].strip() and not lines[n].strip().startswith('- '):
                                    temp_lines.append(lines[n])
                            
                            # Add the tests: line
                            temp_lines.append(' ' * (column_indent + 2) + 'tests:')
                            changes_made.append(f"Added missing 'tests:' line for column at line {k+1}")
                            
                            # Now add the properly indented test
                            temp_lines.append(' ' * (column_indent + 4) + line.strip())
                            changes_made.append(f"Fixed root-level test indentation at line {i+1}: {line.strip()}")
                            
                            # Replace the current line with these temp lines
                            fixed_lines.extend(temp_lines)
                        else:
                            # tests: line exists, just indent the test properly
                            fixed_lines.append(' ' * (column_indent + 4) + line.strip())
                            changes_made.append(f"Fixed root-level test indentation at line {i+1}: {line.strip()}")
                        
                        break
                    k -= 1
                else:
                    # Couldn't find a column, just add the line as-is
                    fixed_lines.append(line)
            else:
                fixed_lines.append(line)
            
            i += 1
        
        content = '\n'.join(fixed_lines)
        
        # Remove excessive empty lines
        content = re.sub(r'\n\s*\n\s*\n+', '\n\n', content)
        
        # Ensure file ends with newline
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
    """Main function to process files with test indentation issues."""
    
    # Get all files that have the problematic pattern
    models_dir = Path("models")
    yaml_files = []
    
    for pattern in ["**/*.yml", "**/*.yaml"]:
        yaml_files.extend(models_dir.glob(pattern))
    
    print(f"[TEST-INDENT] Checking {len(yaml_files)} YAML files for test indentation issues...")
    fixed_count = 0
    
    for yaml_file in yaml_files:
        file_path = str(yaml_file)
        if fix_test_indentation(file_path):
            fixed_count += 1
    
    print(f"\n[DONE] Fixed test indentation in {fixed_count} files")

if __name__ == "__main__":
    main()