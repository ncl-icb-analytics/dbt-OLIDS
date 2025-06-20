#!/usr/bin/env python3
"""
Script to fix disease register YAML files to use standardised column names.

Changes condition-specific column names (e.g., is_on_asthma_register, is_on_diabetes_register) 
to the standardised 'is_on_register' pattern.

Also fixes other column naming inconsistencies like:
- earliest_[condition]_diagnosis_date -> earliest_diagnosis_date
- latest_[condition]_diagnosis_date -> latest_diagnosis_date
- etc.
"""

import os
import re
from pathlib import Path

# Define the mapping of old column patterns to new standardised names
COLUMN_MAPPINGS = {
    # Main register flags
    r'is_on_\w+_register': 'is_on_register',
    
    # Diagnosis date patterns
    r'earliest_\w+_diagnosis_date': 'earliest_diagnosis_date',
    r'latest_\w+_diagnosis_date': 'latest_diagnosis_date',
    r'earliest_\w+_date(?!.*diagnosis)': 'earliest_diagnosis_date',  # e.g., earliest_asthma_date
    r'latest_\w+_date(?!.*diagnosis)': 'latest_diagnosis_date',     # e.g., latest_diabetes_date
    
    # Specific condition patterns that need manual mapping
    'earliest_chd_date': 'earliest_diagnosis_date',
    'latest_chd_date': 'latest_diagnosis_date',
    'earliest_af_diagnosis_date': 'earliest_diagnosis_date',
    'latest_af_diagnosis_date': 'latest_diagnosis_date',
    'earliest_hf_diagnosis_date': 'earliest_diagnosis_date',
    'latest_hf_diagnosis_date': 'latest_diagnosis_date',
    'earliest_htn_diagnosis_date': 'earliest_diagnosis_date',
    'latest_htn_diagnosis_date': 'latest_diagnosis_date',
    'earliest_ckd_diagnosis_date': 'earliest_diagnosis_date',
    'latest_ckd_diagnosis_date': 'latest_diagnosis_date',
    'earliest_ld_diagnosis_date': 'earliest_diagnosis_date',
    'latest_ld_diagnosis_date': 'latest_diagnosis_date',
    'earliest_pad_date': 'earliest_diagnosis_date',
    'latest_pad_date': 'latest_diagnosis_date',
    'earliest_osteoporosis_date': 'earliest_diagnosis_date',
    'latest_osteoporosis_date': 'latest_diagnosis_date',
    'earliest_stroke_tia_date': 'earliest_diagnosis_date',
    'latest_stroke_tia_date': 'latest_diagnosis_date',
    'earliest_palliative_care_date': 'earliest_diagnosis_date',
    'latest_palliative_care_date': 'latest_diagnosis_date',
    'earliest_nafld_diagnosis_date': 'earliest_diagnosis_date',
    'latest_nafld_diagnosis_date': 'latest_diagnosis_date',
    
    # Concept code arrays
    r'all_\w+_concept_codes': 'all_diagnosis_concept_codes',
    r'all_\w+_concept_displays': 'all_diagnosis_concept_displays',
    
    # Special cases
    'sk_patient_id': 'person_id',
    'meets_criteria': 'meets_age_criteria',  # for NAFLD register
}

def fix_yaml_file(file_path):
    """Fix column name references in a single YAML file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Apply regex-based replacements
        for pattern, replacement in COLUMN_MAPPINGS.items():
            if pattern.startswith('r\'') or '\\w' in pattern or '[' in pattern:
                # This is a regex pattern
                content = re.sub(pattern, replacement, content)
            else:
                # This is a literal string replacement
                content = content.replace(pattern, replacement)
        
        # Write back if changed
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"✓ Updated: {file_path}")
            return True
        else:
            print(f"- No changes: {file_path}")
            return False
            
    except Exception as e:
        print(f"✗ Error processing {file_path}: {e}")
        return False

def main():
    """Main function to process all disease register YAML files."""
    base_dir = Path("models/marts/disease_registers")
    
    if not base_dir.exists():
        print(f"Error: Directory {base_dir} does not exist")
        return
    
    yaml_files = list(base_dir.glob("fct_person_*_register.yml"))
    
    if not yaml_files:
        print("No disease register YAML files found")
        return
    
    print(f"Found {len(yaml_files)} disease register YAML files to process...")
    print()
    
    updated_count = 0
    for yaml_file in sorted(yaml_files):
        if fix_yaml_file(yaml_file):
            updated_count += 1
    
    print()
    print(f"Summary: Updated {updated_count} out of {len(yaml_files)} files")

if __name__ == "__main__":
    main() 