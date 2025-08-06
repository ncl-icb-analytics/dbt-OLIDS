#!/usr/bin/env python3
"""
Fix int_foot_examination_all.yml formatting issues.
"""

import re

def fix_foot_examination():
    file_path = "models/intermediate/observations/int_foot_examination_all.yml"
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Fix pattern where config: is indented wrong relative to values:
    # The config: should be at the same indentation as values:
    pattern = r'(\s+)(- accepted_values:\s*\n)\s*\n\s*\n(\s+)values:((?:\n\s+- [^\n]+)+)\n\s*\n(\s+)config:'
    replacement = r'\1\2\3  values:\4\n\3  config:'
    content = re.sub(pattern, replacement, content)
    
    # Also fix cases without the empty lines
    pattern2 = r'(\s+)(- accepted_values:\s*\n)(\s+)values:((?:\n\s+- [^\n]+)+)\n(\s+)config:'
    replacement2 = r'\1\2\3  values:\4\n\3  config:'
    content = re.sub(pattern2, replacement2, content)
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"Fixed {file_path}")

if __name__ == "__main__":
    fix_foot_examination()