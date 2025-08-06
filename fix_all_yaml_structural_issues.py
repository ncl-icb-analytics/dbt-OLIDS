#!/usr/bin/env python3
"""
Comprehensive YAML structural fixer for all remaining issues.
Addresses the widespread YAML structural problems identified by pre-commit hooks.
"""

import os
import re
import yaml
from pathlib import Path

def fix_yaml_comprehensive(file_path: str) -> bool:
    """
    Comprehensively fix YAML structural issues.
    
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
        
        # Split into lines for processing
        lines = content.split('\n')
        fixed_lines = []
        i = 0
        
        while i < len(lines):
            line = lines[i]
            
            # Pattern 1: Fix columns that are not properly indented
            # Look for columns that should be under a parent but aren't properly indented
            if re.match(r'^- name: ', line) and i > 0:
                # Check if this should be under a columns: section
                # Look backward for the most recent columns: or tests: line
                j = i - 1
                found_parent = False
                while j >= 0:
                    prev_line = lines[j]
                    if re.match(r'\s+columns:\s*$', prev_line):
                        # This should be indented as a column
                        expected_indent = len(prev_line) - len(prev_line.lstrip()) + 2
                        current_indent = len(line) - len(line.lstrip()) if line.strip() else 0
                        if current_indent < expected_indent:
                            fixed_lines.append(' ' * expected_indent + line.strip())
                            changes_made.append(f"Fixed column indentation at line {i+1}")
                            found_parent = True
                        break
                    elif re.match(r'\s+tests:\s*$', prev_line):
                        # This might be a model-level test, check if it should be indented
                        expected_indent = len(prev_line) - len(prev_line.lstrip()) + 2
                        current_indent = len(line) - len(line.lstrip()) if line.strip() else 0
                        if current_indent < expected_indent:
                            fixed_lines.append(' ' * expected_indent + line.strip())
                            changes_made.append(f"Fixed test indentation at line {i+1}")
                            found_parent = True
                        break
                    elif prev_line.strip() and not prev_line.strip().startswith('#'):
                        # Found some other content, stop searching
                        break
                    j -= 1
                
                if not found_parent:
                    fixed_lines.append(line)
            
            # Pattern 2: Fix test items that are not properly indented under tests:
            elif re.match(r'^- (not_null|unique|relationships|accepted_values|dbt_utils\.|cluster_ids_exist|bnf_codes_exist)', line):
                # Look backward for the most recent tests: line
                j = i - 1
                found_tests = False
                while j >= 0 and j >= i - 10:  # Don't search too far back
                    prev_line = lines[j]
                    if re.match(r'\s+tests:\s*$', prev_line):
                        expected_indent = len(prev_line) - len(prev_line.lstrip()) + 2
                        fixed_lines.append(' ' * expected_indent + line.strip())
                        changes_made.append(f"Fixed root-level test indentation at line {i+1}")
                        found_tests = True
                        break
                    elif prev_line.strip() and re.match(r'\s+- name:', prev_line):
                        # Found a column, stop searching
                        break
                    j -= 1
                
                if not found_tests:
                    fixed_lines.append(line)
            
            # Pattern 3: Fix malformed description lines that got merged with other content
            elif ':' in line and 'description:' in line and line.strip().count(':') > 1:
                # This might be a malformed line like "description: Something else_key: value"
                parts = line.split('description:', 1)
                if len(parts) == 2:
                    prefix = parts[0]
                    desc_part = parts[1].strip()
                    
                    # Try to separate the description from other content
                    # Look for common patterns that got merged
                    patterns = [r'(\s*[^\s]+.*?)\s+(tests:|data_type:|columns:)', 
                               r'(\s*[^\s]+.*?)\s+(- name:)',
                               r'(\s*[^\s]+.*?)\s+([a-zA-Z_]+:)']
                    
                    fixed = False
                    for pattern in patterns:
                        match = re.match(pattern, desc_part)
                        if match:
                            desc_text = match.group(1).strip()
                            next_content = match.group(2)
                            fixed_lines.append(f"{prefix}description: {desc_text}")
                            # The next content should be on the next line with proper indentation
                            base_indent = len(prefix)
                            fixed_lines.append(' ' * base_indent + next_content)
                            changes_made.append(f"Separated merged content at line {i+1}")
                            fixed = True
                            break
                    
                    if not fixed:
                        fixed_lines.append(line)
                else:
                    fixed_lines.append(line)
            
            # Pattern 4: Fix orphaned content that should be properly indented
            elif line.strip() and not line.startswith(' ') and not line.startswith('#') and ':' in line and not line.startswith('version:') and not line.startswith('models:'):
                # This might be content that should be indented under a parent
                if i > 0:
                    prev_line_idx = i - 1
                    while prev_line_idx >= 0 and not lines[prev_line_idx].strip():
                        prev_line_idx -= 1
                    
                    if prev_line_idx >= 0:
                        prev_line = lines[prev_line_idx]
                        if re.match(r'\s+- name:', prev_line) or re.match(r'\s+tests:', prev_line):
                            # This content should be indented under the previous item
                            expected_indent = len(prev_line) - len(prev_line.lstrip()) + 2
                            fixed_lines.append(' ' * expected_indent + line.strip())
                            changes_made.append(f"Fixed orphaned content indentation at line {i+1}")
                        else:
                            fixed_lines.append(line)
                    else:
                        fixed_lines.append(line)
                else:
                    fixed_lines.append(line)
            
            else:
                fixed_lines.append(line)
            
            i += 1
        
        content = '\n'.join(fixed_lines)
        
        # Additional cleanup patterns
        # Fix excessive empty lines
        content = re.sub(r'\n\s*\n\s*\n+', '\n\n', content)
        
        # Ensure file ends with newline
        if not content.endswith('\n'):
            content += '\n'
        
        # Try to validate the YAML structure
        try:
            yaml.safe_load(content)
        except yaml.YAMLError as e:
            changes_made.append(f"WARNING: Still has YAML errors after fixes: {str(e)}")
        
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"[FIXED] {file_path}")
            for change in changes_made[:5]:  # Show first 5 changes to avoid spam
                print(f"   - {change}")
            if len(changes_made) > 5:
                print(f"   - ... and {len(changes_made) - 5} more changes")
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
    
    print(f"[COMPREHENSIVE] Processing {len(yaml_files)} YAML files for structural issues...")
    fixed_count = 0
    
    for yaml_file in yaml_files:
        file_path = str(yaml_file)
        if fix_yaml_comprehensive(file_path):
            fixed_count += 1
    
    print(f"\n[DONE] Fixed structural issues in {fixed_count} files")

if __name__ == "__main__":
    main()