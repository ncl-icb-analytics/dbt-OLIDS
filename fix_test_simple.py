#!/usr/bin/env python3
"""
Simple targeted fix for dbt test deprecation warnings.
Swaps the order of tests and description when tests come first.
"""

import os
import re
import argparse


def fix_file(file_path: str, dry_run: bool = False) -> bool:
    """Fix a single file by swapping tests and description order."""
    print(f"Processing: {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Pattern to match and swap tests/description order
    # Matches: model name, then tests block, then description
    pattern = r"""
        (- name:\s+\w+\s*\n)           # Model name (group 1)
        (\s+tests:\s*\n                # Tests block start (group 2)
         (?:\s+- [^\n]*\n)*            # Test items (simple format)
         (?:\s+- [^\n]*:\s*\n          # Test items with parameters
          (?:\s+[^\n]*\n)*)*           # Parameter lines
        )
        (\s+description:\s*[^\n]*\n    # Description line (group 3)
         (?:\s+[^\n]*\n)*              # Description content
        )
    """
    
    # Check if pattern exists
    if not re.search(pattern, content, re.VERBOSE | re.DOTALL):
        print("  No deprecated format found")
        return False
    
    print("  Found deprecated test format - fixing...")
    
    # Swap the order: name, description, tests
    def swap_order(match):
        return match.group(1) + match.group(3) + match.group(2)
    
    fixed_content = re.sub(pattern, swap_order, content, flags=re.VERBOSE | re.DOTALL)
    
    if not dry_run:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(fixed_content)
        print("  Fixed")
    else:
        print("  Would fix (dry run)")
    
    return True


# Test with single file first
if __name__ == '__main__':
    test_file = r"models\intermediate\diagnoses\int_familial_hypercholesterolaemia_diagnoses_all.yml"
    fix_file(test_file, dry_run=True)