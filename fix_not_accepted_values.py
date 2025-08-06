#!/usr/bin/env python3
"""
Fix dbt_utils.not_accepted_values with values at wrong indentation.
"""

import re
from pathlib import Path

def fix_not_accepted_values(file_path: str) -> bool:
    """Fix dbt_utils.not_accepted_values pattern."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Pattern: dbt_utils.not_accepted_values with values at wrong indentation
        pattern = r'(\s+- dbt_utils\.not_accepted_values:\s*\n)\s*\n(\s+)values:'
        replacement = r'\1\2arguments:\n\2  values:'
        
        content = re.sub(pattern, replacement, content)
        
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
    files = [
        "models/marts/disease_registers/qof/fct_person_atrial_fibrillation_register.yml",
        "models/marts/disease_registers/qof/fct_person_diabetes_register.yml",
        "models/marts/disease_registers/qof/fct_person_epilepsy_register.yml",
        "models/marts/disease_registers/qof/fct_person_heart_failure_register.yml"
    ]
    
    for file_path in files:
        if Path(file_path).exists():
            print(f"Processing: {file_path}")
            fix_not_accepted_values(file_path)

if __name__ == "__main__":
    main()