#!/usr/bin/env python3
"""
Fix accepted_values test formatting issues - remove extra blank lines and fix indentation.
"""

import re
from pathlib import Path

def fix_accepted_values_formatting(file_path: str) -> bool:
    """Fix accepted_values test formatting."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Pattern 1: Fix accepted_values with blank lines and wrong indentation
        # This matches accepted_values with empty lines and misaligned values
        pattern1 = r'(\s+)(- accepted_values:\s*\n)\s*\n\s*\n?(\s+)values:'
        replacement1 = r'\1\2\1  values:'
        content = re.sub(pattern1, replacement1, content)
        
        # Pattern 2: Fix accepted_values with just one blank line
        pattern2 = r'(\s+)(- accepted_values:\s*\n)\s*\n(\s+)values:'
        replacement2 = r'\1\2\1  values:'
        content = re.sub(pattern2, replacement2, content)
        
        # Pattern 3: Fix quote parameter that's misaligned
        pattern3 = r'(\s+values:(?:\s*\n\s+- [^\n]+)+)\n\s+quote: false'
        replacement3 = r'\1\n        quote: false'
        content = re.sub(pattern3, replacement3, content)
        
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
    """Process all YAML files to fix accepted_values formatting."""
    
    models_dir = Path("models")
    yml_files = list(models_dir.rglob("*.yml"))
    
    fixed_count = 0
    for yml_file in yml_files:
        if fix_accepted_values_formatting(str(yml_file)):
            fixed_count += 1
    
    print(f"\nCompleted processing {len(yml_files)} files")
    print(f"Fixed {fixed_count} files")

if __name__ == "__main__":
    main()