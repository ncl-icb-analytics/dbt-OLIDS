#!/usr/bin/env python3
"""
Comprehensive YAML indentation fixer for dbt test format issues.
Fixes various YAML indentation and structure problems identified by pre-commit hooks.
"""

import os
import re
import yaml
from pathlib import Path
from typing import List, Dict, Any

def fix_yaml_indentation_issues(file_path: str, dry_run: bool = False) -> bool:
    """
    Fix various YAML indentation and structure issues in dbt schema files.
    
    Args:
        file_path: Path to the YAML file to fix
        dry_run: If True, only report what would be changed without making changes
        
    Returns:
        bool: True if changes were made (or would be made in dry_run), False otherwise
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        changes_made = []
        
        # Fix 1: Arguments indentation in dbt_utils.accepted_range tests
        # Pattern: max_value and config not properly indented under arguments
        pattern1 = r'(\s+- dbt_utils\.accepted_range:\s*\n\s+arguments:\s*\n\s+min_value:\s*[^\n]+)\n(\s+)(max_value:\s*[^\n]+)\n(\s+)(config:\s*\n(?:\s+[^\n]+\n)*)'
        def fix_accepted_range_args(match):
            prefix = match.group(1)
            max_value_indent = match.group(2)
            max_value = match.group(3)
            config_indent = match.group(4)
            config_block = match.group(5)
            
            # Ensure max_value and config are indented same as min_value (under arguments)
            base_indent = '                '  # 16 spaces to match min_value indentation
            fixed_max_value = base_indent + max_value
            fixed_config = re.sub(r'^(\s*)', base_indent, config_block, flags=re.MULTILINE)
            
            return f"{prefix}\n{fixed_max_value}\n{fixed_config}"
        
        new_content = re.sub(pattern1, fix_accepted_range_args, content, flags=re.MULTILINE)
        if new_content != content:
            changes_made.append("Fixed dbt_utils.accepted_range arguments indentation")
            content = new_content
        
        # Fix 2: Test indentation issues where tests are not properly aligned
        # Pattern: tests with inconsistent indentation
        pattern2 = r'(\s+tests:\s*\n)(\s+)(- not_null\s*\n)(\s*\n)*(\s*)(- [^\n]+)'
        def fix_test_alignment(match):
            tests_line = match.group(1)
            first_indent = match.group(2)
            first_test = match.group(3)
            blank_lines = match.group(4) or ''
            wrong_indent = match.group(5)
            second_test = match.group(6)
            
            # Ensure all tests use same indentation as first test
            return f"{tests_line}{first_test}{second_test.replace(wrong_indent, first_indent, 1)}"
        
        new_content = re.sub(pattern2, fix_test_alignment, content, flags=re.MULTILINE)
        if new_content != content:
            changes_made.append("Fixed test alignment indentation")
            content = new_content
        
        # Fix 3: Column-level test indentation
        # Ensure tests under columns are properly indented
        pattern3 = r'(\s+- name: [^\n]+\s*\n(?:\s+description: [^\n]*\n)*(?:\s+data_type: [^\n]*\n)*\s*\n)(\s+)(tests:\s*\n)(\s+- [^\n]+(?:\n(?:\s{6,}[^\n-]+)*)*(?:\n\s+- [^\n]+(?:\n(?:\s{6,}[^\n-]+)*)*)*)'
        def fix_column_test_indent(match):
            column_info = match.group(1)
            tests_indent = match.group(2)
            tests_header = match.group(3)
            tests_content = match.group(4)
            
            # Ensure tests are properly indented relative to column
            base_indent = tests_indent + '  '  # 2 more spaces than tests:
            
            # Fix each test line
            lines = tests_content.split('\n')
            fixed_lines = []
            for line in lines:
                if line.strip():
                    if line.strip().startswith('- '):
                        # This is a test item
                        fixed_lines.append(base_indent + line.strip())
                    elif ':' in line and not line.strip().startswith('-'):
                        # This is a test parameter, indent more
                        fixed_lines.append(base_indent + '  ' + line.strip())
                    else:
                        # Other content, maintain relative indentation
                        fixed_lines.append(base_indent + '    ' + line.strip())
                else:
                    fixed_lines.append('')
            
            return column_info + tests_indent + tests_header + '\n'.join(fixed_lines)
        
        # Fix 4: Handle specific YAML structure errors by ensuring proper key-value alignment
        # Look for cases where keys and values are misaligned
        lines = content.split('\n')
        fixed_lines = []
        i = 0
        
        while i < len(lines):
            line = lines[i]
            
            # Check for misaligned key-value pairs
            if ':' in line and not line.strip().startswith('#'):
                # Get the indentation level
                indent = len(line) - len(line.lstrip())
                
                # Check if next line has wrong indentation for a value
                if i + 1 < len(lines):
                    next_line = lines[i + 1]
                    if next_line.strip() and not next_line.strip().startswith('-') and not next_line.strip().startswith('#'):
                        next_indent = len(next_line) - len(next_line.lstrip())
                        
                        # If next line has same or less indentation but contains a value, it might be wrong
                        if ':' not in next_line and next_indent <= indent:
                            # This might be a misaligned value, fix it
                            next_line = ' ' * (indent + 2) + next_line.strip()
                            lines[i + 1] = next_line
                            changes_made.append(f"Fixed key-value alignment at line {i+2}")
            
            fixed_lines.append(line)
            i += 1
        
        content = '\n'.join(lines)
        
        # Fix 5: Remove duplicate empty lines and fix spacing
        content = re.sub(r'\n\s*\n\s*\n', '\n\n', content)
        
        # Fix 6: Ensure proper YAML structure by validating and fixing basic issues
        try:
            # Try to parse as YAML to identify structural issues
            yaml_data = yaml.safe_load(content)
            if yaml_data is None:
                changes_made.append("WARNING: File appears to be empty or invalid YAML")
        except yaml.YAMLError as e:
            changes_made.append(f"WARNING: YAML parsing error: {str(e)}")
            
            # Try basic fixes for common YAML errors
            # Fix missing colons after keys
            content = re.sub(r'^(\s*[a-zA-Z_][a-zA-Z0-9_]*)\s*$', r'\1:', content, flags=re.MULTILINE)
            
            # Fix improperly terminated lists
            content = re.sub(r'^(\s*-\s*)$', r'\1null', content, flags=re.MULTILINE)
        
        if content != original_content:
            if not dry_run:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                print(f"[FIXED] {file_path}")
                for change in changes_made:
                    print(f"   - {change}")
            else:
                print(f"[DRY-RUN] Would fix {file_path}")
                for change in changes_made:
                    print(f"   - {change}")
            return True
        
        return False
        
    except Exception as e:
        print(f"[ERROR] Error processing {file_path}: {str(e)}")
        return False

def main():
    """Main function to process all YAML files."""
    
    # Get all YAML files in the models directory
    models_dir = Path("models")
    yaml_files = []
    
    for pattern in ["**/*.yml", "**/*.yaml"]:
        yaml_files.extend(models_dir.glob(pattern))
    
    print(f"Found {len(yaml_files)} YAML files to process")
    
    # Process files that were specifically mentioned in the error
    priority_files = [
        "models/marts/organisation/dim_pcn.yml",
        "models/staging/flu_staging_schema.yml", 
        "models/marts/programme/ltc_lcs/cf/fct_ltc_lcs_person_dashboard.yml",
        "models/marts/organisation/dim_practice_neighbourhood.yml",
        "models/marts/programme/ltc_lcs/cf/fct_ltc_lcs_population_summary.yml",
        "models/marts/organisation/dim_person_historical_practice.yml",
        "models/marts/programme/flu/flu_marts_schema.yml"
    ]
    
    # Process priority files first
    print("\n[PRIORITY] Processing priority files with known errors...")
    fixed_count = 0
    for file_path in priority_files:
        if os.path.exists(file_path):
            if fix_yaml_indentation_issues(file_path, dry_run=False):
                fixed_count += 1
    
    # Then process all other YAML files
    print(f"\n[ALL] Processing all YAML files...")
    for yaml_file in yaml_files:
        file_path = str(yaml_file)
        if file_path not in priority_files:  # Skip already processed priority files
            if fix_yaml_indentation_issues(file_path, dry_run=False):
                fixed_count += 1
    
    print(f"\n[DONE] Completed! Fixed {fixed_count} files")

if __name__ == "__main__":
    main()