#!/usr/bin/env python3
"""
Comprehensive YAML indentation fix for dbt files.
Fixes quote: false and config: parameters at wrong indentation levels.
"""

import re
from pathlib import Path

def fix_yaml_indentation(file_path: str) -> bool:
    """
    Fix YAML indentation issues, particularly for accepted_values tests.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        fixed_lines = []
        i = 0
        changes_made = False
        
        while i < len(lines):
            line = lines[i]
            
            # Check if this is an accepted_values test
            if '- accepted_values:' in line:
                base_indent = len(line) - len(line.lstrip())
                fixed_lines.append(line)
                i += 1
                
                # Skip any blank lines
                while i < len(lines) and lines[i].strip() == '':
                    fixed_lines.append(lines[i])
                    i += 1
                
                # Now we should be at the values: line or another parameter
                if i < len(lines):
                    # Process all parameters under accepted_values
                    expected_indent = base_indent + 4  # accepted_values parameters should be indented 4 spaces from the test
                    
                    while i < len(lines):
                        current_line = lines[i]
                        stripped = current_line.strip()
                        
                        # Check if we've reached the next column or test
                        if stripped.startswith('- name:') or stripped.startswith('- ') and 'accepted_values' not in current_line:
                            break
                        
                        # Handle values: parameter
                        if stripped.startswith('values:'):
                            # values: should be at expected_indent
                            fixed_lines.append(' ' * expected_indent + 'values:\n')
                            changes_made = True
                            i += 1
                            
                            # Process value list items
                            while i < len(lines):
                                val_line = lines[i]
                                val_stripped = val_line.strip()
                                
                                if val_stripped.startswith('- '):
                                    # List item under values
                                    fixed_lines.append(' ' * (expected_indent + 4) + val_stripped + '\n')
                                    i += 1
                                elif val_stripped in ['quote: false', 'quote: true']:
                                    # quote parameter should be at same level as values:
                                    fixed_lines.append(' ' * expected_indent + val_stripped + '\n')
                                    changes_made = True
                                    i += 1
                                    break
                                elif val_stripped.startswith('config:'):
                                    # config: should be at same level as values:
                                    fixed_lines.append(' ' * expected_indent + 'config:\n')
                                    changes_made = True
                                    i += 1
                                    # Handle config sub-parameters
                                    if i < len(lines) and lines[i].strip().startswith('where:'):
                                        fixed_lines.append(' ' * (expected_indent + 2) + lines[i].strip() + '\n')
                                        i += 1
                                    break
                                elif val_stripped == '':
                                    # Blank line
                                    fixed_lines.append(val_line)
                                    i += 1
                                else:
                                    # End of values list
                                    break
                        
                        # Handle quote: or config: that appear directly under accepted_values
                        elif stripped in ['quote: false', 'quote: true']:
                            fixed_lines.append(' ' * expected_indent + stripped + '\n')
                            changes_made = True
                            i += 1
                        elif stripped.startswith('config:'):
                            fixed_lines.append(' ' * expected_indent + 'config:\n')
                            changes_made = True
                            i += 1
                            # Handle config sub-parameters
                            if i < len(lines) and lines[i].strip().startswith('where:'):
                                fixed_lines.append(' ' * (expected_indent + 2) + lines[i].strip() + '\n')
                                i += 1
                        else:
                            fixed_lines.append(current_line)
                            i += 1
            else:
                fixed_lines.append(line)
                i += 1
        
        if changes_made:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.writelines(fixed_lines)
            print(f"  Fixed {Path(file_path).name}")
            return True
        return False
    
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def main():
    """Process all YAML files to fix indentation issues."""
    
    models_dir = Path("models")
    yml_files = list(models_dir.rglob("*.yml"))
    
    print(f"Found {len(yml_files)} YAML files")
    print("\nFixing indentation issues...")
    
    fixed_count = 0
    for yml_file in yml_files:
        if fix_yaml_indentation(str(yml_file)):
            fixed_count += 1
    
    print(f"\nCompleted processing {len(yml_files)} files")
    print(f"Fixed {fixed_count} files")

if __name__ == "__main__":
    main()