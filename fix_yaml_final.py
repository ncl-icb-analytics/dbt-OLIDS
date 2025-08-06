#!/usr/bin/env python3
"""
Fix YAML indentation issues in all files.
"""

import os
import re


def fix_yaml_indentation_in_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Pattern to fix combination_of_columns with misaligned items
        original_content = content
        
        # Fix pattern: arguments -> combination_of_columns -> misaligned list items
        lines = content.split('\n')
        fixed_lines = []
        i = 0
        
        while i < len(lines):
            line = lines[i]
            fixed_lines.append(line)
            
            # Look for combination_of_columns
            if 'combination_of_columns:' in line and line.strip().endswith(':'):
                combo_indent = len(line) - len(line.lstrip())
                list_indent = ' ' * (combo_indent + 2)  # Proper indentation for list items
                
                # Process following lines that should be list items
                j = i + 1
                while j < len(lines) and j < i + 10:  # Look ahead max 10 lines
                    next_line = lines[j]
                    
                    if next_line.strip() == '':
                        fixed_lines.append(next_line)
                        j += 1
                        continue
                    
                    # If it's a list item but not properly indented
                    if next_line.strip().startswith('- ') and not next_line.startswith(list_indent):
                        # Fix the indentation
                        fixed_line = list_indent + next_line.lstrip()
                        fixed_lines.append(fixed_line)
                        j += 1
                        continue
                    
                    # If it's not a list item or properly indented, we're done
                    break
                
                i = j - 1  # Continue from where we left off
            
            i += 1
        
        fixed_content = '\n'.join(fixed_lines)
        
        if fixed_content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(fixed_content)
            print(f'Fixed: {file_path}')
            return True
        
        return False
        
    except Exception as e:
        print(f'Error processing {file_path}: {e}')
        return False


def main():
    fixed_count = 0
    
    for root, dirs, files in os.walk('models'):
        for file in files:
            if file.endswith('.yml'):
                file_path = os.path.join(root, file)
                if fix_yaml_indentation_in_file(file_path):
                    fixed_count += 1
    
    print(f'Total files fixed: {fixed_count}')


if __name__ == '__main__':
    main()