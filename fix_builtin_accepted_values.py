#!/usr/bin/env python3
"""
Fix built-in accepted_values test that incorrectly has arguments: wrapper.
The built-in accepted_values test (without dbt_utils prefix) expects values directly.
"""

import re
from pathlib import Path

def fix_builtin_accepted_values(file_path: str) -> bool:
    """Remove arguments: wrapper from built-in accepted_values tests."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Pattern: accepted_values (not dbt_utils.accepted_values) with arguments: wrapper
        # This matches the built-in test that shouldn't have arguments: wrapper
        pattern = r'(\s+)(- accepted_values:\s*\n\s*\n?\s*)arguments:\s*\n(\s+)(values:(?:\n\s+- [^\n]+)+)'
        
        def replace_func(match):
            indent = match.group(1)
            test_line = match.group(2)
            values_block = match.group(4)
            
            # Remove the arguments: wrapper and dedent the values block
            return f"{indent}{test_line}{indent}  {values_block}"
        
        content = re.sub(pattern, replace_func, content)
        
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"  Fixed {Path(file_path).name}")
            return True
        return False
    
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def main():
    """Process all YAML files to fix built-in accepted_values tests."""
    
    models_dir = Path("models")
    yml_files = list(models_dir.rglob("*.yml"))
    
    fixed_count = 0
    for yml_file in yml_files:
        if fix_builtin_accepted_values(str(yml_file)):
            fixed_count += 1
            print(f"Fixed: {yml_file}")
    
    print(f"\nCompleted processing {len(yml_files)} files")
    print(f"Fixed {fixed_count} files")

if __name__ == "__main__":
    main()