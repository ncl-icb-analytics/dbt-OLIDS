#!/usr/bin/env python3
"""
Script to standardise column naming across all disease register models.
Changes all variations to use consistent 'earliest_diagnosis_date', 'latest_diagnosis_date', 
'earliest_resolved_date' and 'latest_resolved_date' columns.
"""

import os
import re
import glob
from pathlib import Path

def standardise_register_files():
    """Standardise column naming in all register files"""
    
    # Get all register files
    register_files = glob.glob("models/marts/disease_registers/fct_person_*_register.sql")
    
    # Define column mappings - map old patterns to new standardised names
    replacements = {
        # Earliest diagnosis date patterns
        'earliest_af_diagnosis_date': 'earliest_diagnosis_date',
        'earliest_asthma_diagnosis_date': 'earliest_diagnosis_date',
        'earliest_cancer_diagnosis_date': 'earliest_diagnosis_date',
        'earliest_chd_diagnosis_date': 'earliest_diagnosis_date',
        'earliest_ckd_diagnosis_date': 'earliest_diagnosis_date',
        'earliest_copd_diagnosis_date': 'earliest_diagnosis_date',
        'earliest_cyp_asthma_diagnosis_date': 'earliest_diagnosis_date',
        'earliest_dementia_diagnosis_date': 'earliest_diagnosis_date',
        'earliest_depression_diagnosis_date': 'earliest_diagnosis_date',
        'earliest_diabetes_diagnosis_date': 'earliest_diagnosis_date',
        'earliest_epilepsy_diagnosis_date': 'earliest_diagnosis_date',
        'earliest_fhyp_diagnosis_date': 'earliest_diagnosis_date',
        'earliest_heart_failure_diagnosis_date': 'earliest_diagnosis_date',
        'earliest_hypertension_diagnosis_date': 'earliest_diagnosis_date',
        'earliest_learning_disability_diagnosis_date': 'earliest_diagnosis_date',
        'earliest_osteoporosis_diagnosis_date': 'earliest_diagnosis_date',
        'earliest_pad_diagnosis_date': 'earliest_diagnosis_date',
        'earliest_palliative_care_diagnosis_date': 'earliest_diagnosis_date',
        'earliest_ra_diagnosis_date': 'earliest_diagnosis_date',
        'earliest_smi_diagnosis_date': 'earliest_diagnosis_date',
        # These don't follow diagnosis pattern but should be standardised
        'earliest_nafld_date': 'earliest_diagnosis_date',
        'earliest_stroke_tia_date': 'earliest_diagnosis_date',
        'earliest_chd_date': 'earliest_diagnosis_date',
        
        # Latest diagnosis date patterns  
        'latest_af_diagnosis_date': 'latest_diagnosis_date',
        'latest_asthma_diagnosis_date': 'latest_diagnosis_date',
        'latest_cancer_diagnosis_date': 'latest_diagnosis_date',
        'latest_chd_diagnosis_date': 'latest_diagnosis_date',
        'latest_ckd_diagnosis_date': 'latest_diagnosis_date',
        'latest_copd_diagnosis_date': 'latest_diagnosis_date',
        'latest_cyp_asthma_diagnosis_date': 'latest_diagnosis_date',
        'latest_dementia_diagnosis_date': 'latest_diagnosis_date',
        'latest_depression_diagnosis_date': 'latest_diagnosis_date',
        'latest_diabetes_diagnosis_date': 'latest_diagnosis_date',
        'latest_epilepsy_diagnosis_date': 'latest_diagnosis_date',
        'latest_fhyp_diagnosis_date': 'latest_diagnosis_date',
        'latest_heart_failure_diagnosis_date': 'latest_diagnosis_date',
        'latest_hypertension_diagnosis_date': 'latest_diagnosis_date',
        'latest_learning_disability_diagnosis_date': 'latest_diagnosis_date',
        'latest_osteoporosis_diagnosis_date': 'latest_diagnosis_date',
        'latest_pad_diagnosis_date': 'latest_diagnosis_date',
        'latest_palliative_care_diagnosis_date': 'latest_diagnosis_date',
        'latest_ra_diagnosis_date': 'latest_diagnosis_date',
        'latest_smi_diagnosis_date': 'latest_diagnosis_date',
        # These don't follow diagnosis pattern but should be standardised
        'latest_nafld_date': 'latest_diagnosis_date',
        'latest_stroke_tia_date': 'latest_diagnosis_date',
        'latest_chd_date': 'latest_diagnosis_date',
        
        # Earliest resolved date patterns
        'earliest_resolution_date': 'earliest_resolved_date',
        
        # Latest resolved date patterns
        'latest_af_resolved_date': 'latest_resolved_date',
        'latest_asthma_resolved_date': 'latest_resolved_date', 
        'latest_cancer_resolved_date': 'latest_resolved_date',
        'latest_ckd_resolved_date': 'latest_resolved_date',
        'latest_copd_resolved_date': 'latest_resolved_date',
        'latest_dementia_resolved_date': 'latest_resolved_date',
        'latest_depression_resolved_date': 'latest_resolved_date',
        'latest_epilepsy_resolved_date': 'latest_resolved_date',
        'latest_hf_resolved_date': 'latest_resolved_date',
        'latest_ld_resolved_date': 'latest_resolved_date',
        'latest_resolution_date': 'latest_resolved_date',
        'latest_smi_resolved_date': 'latest_resolved_date',
    }
    
    updated_files = []
    
    for file_path in register_files:
        print(f"Processing: {file_path}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        
        # Apply replacements
        for old_name, new_name in replacements.items():
            content = content.replace(old_name, new_name)
        
        # Check if file was modified
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            updated_files.append(file_path)
            print(f"  ✓ Updated")
        else:
            print(f"  - No changes needed")
    
    return updated_files

def update_ltc_summary():
    """Update the LTC summary file to use standardised column names"""
    
    ltc_file = "models/marts/disease_registers/fct_person_ltc_summary.sql"
    
    with open(ltc_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original_content = content
    
    # Replace all condition-specific column references with standardised names
    # This is a comprehensive replacement to ensure all references are updated
    standardised_content = content.replace(
        'earliest_af_diagnosis_date', 'earliest_diagnosis_date'
    ).replace(
        'latest_af_diagnosis_date', 'latest_diagnosis_date'
    ).replace(
        'earliest_asthma_diagnosis_date', 'earliest_diagnosis_date'
    ).replace(
        'latest_asthma_diagnosis_date', 'latest_diagnosis_date'
    ).replace(
        'earliest_cancer_diagnosis_date', 'earliest_diagnosis_date'
    ).replace(
        'latest_cancer_diagnosis_date', 'latest_diagnosis_date'
    )
    
    # Add resolved date columns to the template since they might be needed
    # Update the final select to include resolved dates
    if 'latest_diagnosis_date' in standardised_content and 'latest_resolved_date' not in standardised_content:
        # Add resolved date column to the union template if not already present
        standardised_content = standardised_content.replace(
            'latest_diagnosis_date\n    FROM',
            'latest_diagnosis_date,\n        NULL AS latest_resolved_date\n    FROM'
        )
    
    if standardised_content != original_content:
        with open(ltc_file, 'w', encoding='utf-8') as f:
            f.write(standardised_content)
        print(f"Updated LTC summary file: {ltc_file}")
        return True
    else:
        print(f"No changes needed for LTC summary file: {ltc_file}")
        return False

if __name__ == "__main__":
    print("Standardising disease register column naming...")
    print("=" * 50)
    
    # Update register files
    updated_register_files = standardise_register_files()
    
    print("\n" + "=" * 50)
    print(f"Summary: Updated {len(updated_register_files)} register files")
    
    # Update LTC summary
    print("\nUpdating LTC summary file...")
    ltc_updated = update_ltc_summary()
    
    if updated_register_files or ltc_updated:
        print("\n✅ Standardisation complete!")
        print("\nUpdated files:")
        for file_path in updated_register_files:
            print(f"  - {file_path}")
        if ltc_updated:
            print(f"  - models/marts/disease_registers/fct_person_ltc_summary.sql")
    else:
        print("\n✅ All files already using standardised naming!") 