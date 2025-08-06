#!/usr/bin/env python3
"""
Direct fix for test deprecation warnings - exact string replacement.
"""

import re

def fix_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Look for the exact pattern we need to fix
    # Model name, then tests block, then description
    old_pattern = """- name: int_familial_hypercholesterolaemia_diagnoses_all
  tests:
  - cluster_ids_exist:
      cluster_ids: FHYP_COD
  description:"""
    
    new_pattern = """- name: int_familial_hypercholesterolaemia_diagnoses_all
  description:"""
    
    if old_pattern in content:
        print("Found pattern to fix")
        # Replace the pattern
        fixed_content = content.replace(old_pattern, new_pattern)
        
        # Now we need to add the tests block after the description ends
        # Find where the description block ends (before columns:)
        desc_end_pattern = r"(• Supports intensive cholesterol management protocols'\n\n)"
        tests_block = """  tests:
  - cluster_ids_exist:
      cluster_ids: FHYP_COD

"""
        
        fixed_content = re.sub(desc_end_pattern, r'\1' + tests_block, fixed_content)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(fixed_content)
        
        print("Fixed!")
        return True
    else:
        print("Pattern not found")
        return False

if __name__ == '__main__':
    filepath = r"models\intermediate\diagnoses\int_familial_hypercholesterolaemia_diagnoses_all.yml"
    fix_file(filepath)