#!/usr/bin/env python3
"""
Fix YAML files with missing dash indicators in block collections.
Addresses the "did not find expected '-' indicator" error.
"""

import os
import re
from pathlib import Path

def fix_dash_indicators(file_path: str) -> bool:
    """
    Fix YAML files where column definitions are missing proper list indicators.
    
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
            
            # Look for columns: section
            if re.match(r'\s+columns:\s*$', line):
                fixed_lines.append(line)
                columns_indent = len(line) - len(line.lstrip())
                expected_column_indent = columns_indent + 2
                
                j = i + 1
                while j < len(lines):
                    next_line = lines[j]
                    
                    # Skip empty lines
                    if not next_line.strip():
                        fixed_lines.append(next_line)
                        j += 1
                        continue
                    
                    # Check if this should be a column but is missing the dash
                    if (next_line.strip().startswith('name:') and 
                        len(next_line) - len(next_line.lstrip()) == expected_column_indent):
                        # This is a column definition missing the dash
                        indent_spaces = ' ' * expected_column_indent
                        fixed_line = indent_spaces + '- ' + next_line.strip()
                        fixed_lines.append(fixed_line)
                        changes_made.append(f"Added missing dash indicator at line {j+1}: {next_line.strip()}")
                        j += 1
                        continue
                    
                    # Check for properly formatted columns
                    if re.match(r'\s+- name:', next_line):
                        current_indent = len(next_line) - len(next_line.lstrip())
                        if current_indent == expected_column_indent:
                            # This is properly formatted
                            fixed_lines.append(next_line)
                        else:
                            # Fix indentation
                            fixed_lines.append(' ' * expected_column_indent + next_line.strip())
                            changes_made.append(f"Fixed column indentation at line {j+1}")
                        j += 1
                        continue
                    
                    # Check if we've reached the end of columns section
                    if (re.match(r'\s+\w+:', next_line) and 
                        not next_line.strip().startswith('description:') and
                        not next_line.strip().startswith('data_type:') and
                        not next_line.strip().startswith('tests:')):
                        # This is a new section, stop processing columns
                        break
                    
                    # Regular content - add as is
                    fixed_lines.append(next_line)
                    j += 1
                
                i = j - 1
            else:
                fixed_lines.append(line)
            
            i += 1
        
        content = '\n'.join(fixed_lines)
        
        # Additional fix: ensure proper indentation structure
        # Look for cases where description/tests are at wrong level after column names
        lines = content.split('\n')
        fixed_lines = []
        i = 0
        
        while i < len(lines):
            line = lines[i]
            fixed_lines.append(line)
            
            # If this is a column definition, ensure following content is properly indented
            if re.match(r'\s+- name:', line):
                column_indent = len(line) - len(line.lstrip())
                expected_content_indent = column_indent + 2
                
                j = i + 1
                # Process content that belongs to this column
                while j < len(lines):
                    next_line = lines[j]
                    
                    # Empty line
                    if not next_line.strip():
                        fixed_lines.append(next_line)
                        j += 1
                        continue
                    
                    # If this is another column or section, stop
                    if (re.match(r'\s+- name:', next_line) or 
                        (re.match(r'\s+\w+:', next_line) and 
                         not next_line.strip().startswith('description:') and
                         not next_line.strip().startswith('data_type:') and
                         not next_line.strip().startswith('tests:'))):
                        break
                    
                    # This should be content under the column
                    if (next_line.strip().startswith('description:') or 
                        next_line.strip().startswith('data_type:') or
                        next_line.strip().startswith('tests:')):
                        current_indent = len(next_line) - len(next_line.lstrip())
                        if current_indent != expected_content_indent:
                            fixed_lines.append(' ' * expected_content_indent + next_line.strip())
                            changes_made.append(f"Fixed content indentation at line {j+1}")
                        else:
                            fixed_lines.append(next_line)
                    else:
                        # Other content, preserve as-is
                        fixed_lines.append(next_line)
                    
                    j += 1
                
                i = j - 1
            
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
    
    print(f"[DASH-INDICATORS] Processing {len(yaml_files)} YAML files for missing dash indicators...")
    fixed_count = 0
    
    # Sort files to process them in a predictable order
    yaml_files.sort()
    
    for yaml_file in yaml_files:
        file_path = str(yaml_file)
        if fix_dash_indicators(file_path):
            fixed_count += 1
    
    print(f"\n[DONE] Fixed dash indicator issues in {fixed_count} files")

if __name__ == "__main__":
    main()