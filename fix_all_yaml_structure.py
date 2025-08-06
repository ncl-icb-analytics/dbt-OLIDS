#!/usr/bin/env python3
"""
Comprehensive script to fix all YAML structural issues in dbt model files.
Fixes indentation problems with accepted_values tests.
"""

import re
from pathlib import Path
import yaml

def fix_yaml_structure(file_path: str) -> bool:
    """Fix YAML structural issues in a single file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Pattern 1: Fix accepted_values with empty lines between test and values
        # This handles cases with 1-3 empty lines
        pattern1 = r'(\s+)(- accepted_values:\s*\n)(?:\s*\n){1,3}(\s+)values:'
        replacement1 = r'\1\2\1  values:'
        content = re.sub(pattern1, replacement1, content)
        
        # Pattern 2: Fix quote parameter that's indented too far
        # quote: should be at same level as values:
        pattern2 = r'(\s+values:(?:\s*\n\s+- [^\n]+)+)\n\s+quote: false'
        replacement2 = r'\1\n      quote: false'
        content = re.sub(pattern2, replacement2, content)
        
        # Pattern 3: Fix config parameter that's indented too far
        # config: should be at same level as values:
        pattern3 = r'(\s+values:(?:\s*\n\s+- [^\n]+)+)\n\s+config:'
        replacement3 = r'\1\n      config:'
        content = re.sub(pattern3, replacement3, content)
        
        # Pattern 4: Fix cases where quote/config are indented with values list items
        # This is for cases like:
        #   values:
        #     - item1
        #     - item2
        #     quote: false  <-- wrong, should be at values level
        pattern4 = r'(\s+)(values:\s*\n(?:\s+- [^\n]+\n)+)(\s+)(quote: false|config: [^\n]+)'
        def fix_indent(match):
            indent = match.group(1)
            values_block = match.group(2)
            param = match.group(4)
            # The parameter should be at same indent as 'values:'
            return f"{indent}{values_block}{indent}  {param}"
        content = re.sub(pattern4, fix_indent, content)
        
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"  Fixed {Path(file_path).name}")
            return True
        return False
    
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def validate_yaml(file_path: str) -> bool:
    """Validate that a YAML file can be parsed."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            yaml.safe_load(f)
        return True
    except yaml.YAMLError as e:
        print(f"  YAML Error in {Path(file_path).name}: {e}")
        return False

def main():
    """Process all YAML files to fix structural issues."""
    
    models_dir = Path("models")
    yml_files = list(models_dir.rglob("*.yml"))
    
    print(f"Found {len(yml_files)} YAML files")
    print("\nFixing structural issues...")
    
    fixed_count = 0
    invalid_count = 0
    
    for yml_file in yml_files:
        if fix_yaml_structure(str(yml_file)):
            fixed_count += 1
            # Validate the fixed file
            if not validate_yaml(str(yml_file)):
                invalid_count += 1
    
    print(f"\nCompleted processing {len(yml_files)} files")
    print(f"Fixed {fixed_count} files")
    if invalid_count > 0:
        print(f"WARNING: {invalid_count} files still have YAML errors after fixing")
    
    # Check dbt-specific patterns
    print("\nChecking for dbt-specific issues...")
    check_dbt_patterns(yml_files)

def check_dbt_patterns(yml_files):
    """Check for common dbt YAML patterns that cause issues."""
    issues = []
    
    for yml_file in yml_files:
        with open(yml_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Check for quote: false at wrong indentation
        if re.search(r'^\s+- [^\n]+\n\s+quote: false', content, re.MULTILINE):
            issues.append(f"{yml_file}: quote parameter might be at wrong indentation")
        
        # Check for config: at wrong indentation  
        if re.search(r'^\s+- [^\n]+\n\s+config:', content, re.MULTILINE):
            issues.append(f"{yml_file}: config parameter might be at wrong indentation")
    
    if issues:
        print("\nPotential issues found:")
        for issue in issues[:10]:  # Show first 10
            print(f"  {issue}")
        if len(issues) > 10:
            print(f"  ... and {len(issues) - 10} more")

if __name__ == "__main__":
    main()