#!/usr/bin/env python3
"""
Automated fix for YAML column indentation issues.
Fixes the pattern where '- name:' entries are at wrong indentation level under columns.
"""

import os
import re
from pathlib import Path

def fix_column_indentation(file_path: str) -> bool:
    """
    Fix column indentation issues where columns are not properly indented under 'columns:'.
    
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
            
            # Look for the specific pattern: columns: followed by improperly indented column entries
            if re.match(r'\s+columns:\s*$', line):
                fixed_lines.append(line)
                columns_indent = len(line) - len(line.lstrip())
                expected_column_indent = columns_indent + 2
                
                j = i + 1
                # Process following lines until we find something that's not a column or related content
                while j < len(lines):
                    next_line = lines[j]
                    
                    # Skip empty lines
                    if not next_line.strip():
                        fixed_lines.append(next_line)
                        j += 1
                        continue
                    
                    # Check if this is a column definition that needs fixing
                    if re.match(r'^- name:', next_line):
                        # This is a root-level column that should be indented under columns:
                        fixed_lines.append(' ' * expected_column_indent + next_line.strip())
                        changes_made.append(f"Fixed column indentation at line {j+1}: {next_line.strip()}")
                        j += 1
                        
                        # Now handle the content of this column (description, tests, etc.)
                        while j < len(lines):
                            col_content_line = lines[j]
                            
                            # Empty line
                            if not col_content_line.strip():
                                fixed_lines.append(col_content_line)
                                j += 1
                                continue
                            
                            # If this is another column definition or end of columns section
                            if (re.match(r'^- name:', col_content_line) or 
                                re.match(r'\s+\w+:', col_content_line) and not col_content_line.strip().startswith('description:') and not col_content_line.strip().startswith('data_type:') and not col_content_line.strip().startswith('tests:')):
                                # Don't consume this line, let the outer loop handle it
                                break
                            
                            # This is content that belongs to the current column
                            if col_content_line.startswith('    ') or col_content_line.startswith('\t'):
                                # Already properly indented
                                fixed_lines.append(col_content_line)
                            else:
                                # Need to indent properly under the column
                                fixed_lines.append(' ' * (expected_column_indent + 2) + col_content_line.strip())
                                if col_content_line.strip():
                                    changes_made.append(f"Fixed column content indentation at line {j+1}")
                            j += 1
                        
                        # Don't increment j here as the outer while loop will handle the next line
                        j -= 1
                    
                    # Check if this is a properly indented column
                    elif re.match(r'\s+- name:', next_line):
                        current_indent = len(next_line) - len(next_line.lstrip())
                        if current_indent == expected_column_indent:
                            # This is properly indented, just add it
                            fixed_lines.append(next_line)
                        else:
                            # Adjust indentation
                            fixed_lines.append(' ' * expected_column_indent + next_line.strip())
                            changes_made.append(f"Adjusted column indentation at line {j+1}")
                    
                    # Check if we've reached the end of the columns section
                    elif (re.match(r'\s+\w+:', next_line) and 
                          not next_line.strip().startswith('description:') and 
                          not next_line.strip().startswith('data_type:') and 
                          not next_line.strip().startswith('tests:')):
                        # This looks like a new section (tests:, etc.), stop processing columns
                        break
                    
                    else:
                        # Other content, just add it
                        fixed_lines.append(next_line)
                    
                    j += 1
                
                i = j - 1  # Set i to the last processed line
            else:
                fixed_lines.append(line)
            
            i += 1
        
        content = '\n'.join(fixed_lines)
        
        # Clean up excessive empty lines
        content = re.sub(r'\n\s*\n\s*\n+', '\n\n', content)
        
        # Ensure file ends with newline
        if not content.endswith('\n'):
            content += '\n'
        
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"[FIXED] {file_path}")
            for change in changes_made[:3]:  # Show first 3 changes
                print(f"   - {change}")
            if len(changes_made) > 3:
                print(f"   - ... and {len(changes_made) - 3} more changes")
            return True
        
        return False
        
    except Exception as e:
        print(f"[ERROR] Error processing {file_path}: {str(e)}")
        return False

def main():
    """Main function to process all YAML files."""
    
    # Get all YAML files
    models_dir = Path("models")
    yaml_files = []
    
    for pattern in ["**/*.yml", "**/*.yaml"]:
        yaml_files.extend(models_dir.glob(pattern))
    
    print(f"[COLUMN-INDENT] Processing {len(yaml_files)} YAML files for column indentation issues...")
    fixed_count = 0
    
    # Sort files to process them in a predictable order
    yaml_files.sort()
    
    for yaml_file in yaml_files:
        file_path = str(yaml_file)
        if fix_column_indentation(file_path):
            fixed_count += 1
    
    print(f"\n[DONE] Fixed column indentation in {fixed_count} files")

if __name__ == "__main__":
    main()