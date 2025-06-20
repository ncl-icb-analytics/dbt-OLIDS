#!/usr/bin/env python3
"""
Script to standardise column naming across all disease register models.
Changes all variations to use consistent 'earliest_diagnosis_date' and 'latest_diagnosis_date' columns.
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
        # Earliest date patterns
        'earliest_af_diagnosis_date': 'earliest_diagnosis_date',
        'earliest_asthma_diagnosis_date': 'earliest_diagnosis_date',
        'earliest_cancer_diagnosis_date': 'earliest_diagnosis_date',
        'earliest_chd_date': 'earliest_diagnosis_date', 
        'earliest_ckd_diagnosis_date': 'earliest_diagnosis_date',
        'earliest_dementia_diagnosis_date': 'earliest_diagnosis_date',
        'earliest_depression_diagnosis_date': 'earliest_diagnosis_date',
        'earliest_diabetes_date': 'earliest_diagnosis_date',
        'earliest_epilepsy_diagnosis_date': 'earliest_diagnosis_date',
        'earliest_fh_date': 'earliest_diagnosis_date',
        'earliest_gestational_diabetes_date': 'earliest_diagnosis_date',
        'earliest_hf_diagnosis_date': 'earliest_diagnosis_date',
        'earliest_htn_diagnosis_date': 'earliest_diagnosis_date',
        'earliest_ld_diagnosis_date': 'earliest_diagnosis_date',
        'earliest_nafld_date': 'earliest_diagnosis_date',
        'earliest_any_ndh_date': 'earliest_diagnosis_date',
        'earliest_osteoporosis_date': 'earliest_diagnosis_date',
        'earliest_pad_date': 'earliest_diagnosis_date',
        'earliest_palliative_care_date': 'earliest_diagnosis_date',
        'earliest_ra_date': 'earliest_diagnosis_date',
        'earliest_smi_diagnosis_date': 'earliest_diagnosis_date',
        'earliest_stroke_tia_date': 'earliest_diagnosis_date',
        
        # Latest date patterns
        'latest_af_diagnosis_date': 'latest_diagnosis_date',
        'latest_asthma_diagnosis_date': 'latest_diagnosis_date',
        'latest_cancer_diagnosis_date': 'latest_diagnosis_date',
        'latest_chd_date': 'latest_diagnosis_date',
        'latest_ckd_diagnosis_date': 'latest_diagnosis_date',
        'latest_dementia_diagnosis_date': 'latest_diagnosis_date',
        'latest_depression_diagnosis_date': 'latest_diagnosis_date',
        'latest_diabetes_date': 'latest_diagnosis_date',
        'latest_epilepsy_diagnosis_date': 'latest_diagnosis_date',
        'latest_fh_date': 'latest_diagnosis_date',
        'latest_gestational_diabetes_date': 'latest_diagnosis_date',
        'latest_hf_diagnosis_date': 'latest_diagnosis_date',
        'latest_htn_diagnosis_date': 'latest_diagnosis_date',
        'latest_ld_diagnosis_date': 'latest_diagnosis_date',
        'latest_nafld_date': 'latest_diagnosis_date',
        'latest_any_ndh_date': 'latest_diagnosis_date',
        'latest_osteoporosis_date': 'latest_diagnosis_date',
        'latest_pad_date': 'latest_diagnosis_date',
        'latest_palliative_care_date': 'latest_diagnosis_date',
        'latest_ra_date': 'latest_diagnosis_date',
        'latest_smi_diagnosis_date': 'latest_diagnosis_date',
        'latest_stroke_tia_date': 'latest_diagnosis_date',
    }
    
    changes_made = {}
    
    for file_path in register_files:
        print(f"Processing {file_path}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original_content = content
        file_changes = 0
        
        # Apply replacements
        for old_name, new_name in replacements.items():
            # Count how many replacements we make
            before_count = content.count(old_name)
            content = content.replace(old_name, new_name)
            after_count = content.count(old_name)
            
            replacements_made = before_count - after_count
            if replacements_made > 0:
                file_changes += replacements_made
                print(f"  - Replaced {replacements_made} instances of '{old_name}' -> '{new_name}'")
        
        # Write back if changes were made
        if content != original_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(content)
            changes_made[file_path] = file_changes
            print(f"  ✓ Updated {file_path} ({file_changes} changes)")
        else:
            print(f"  - No changes needed for {file_path}")
    
    return changes_made

def update_ltc_summary():
    """Update the LTC summary model to use standardised column names"""
    
    ltc_file = "models/marts/disease_registers/fct_person_ltc_summary.sql"
    
    # Read the current content
    with open(ltc_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Define the replacement pattern for the summary
    # We want to replace all condition-specific column names with the standardised ones
    replacements = {
        'earliest_af_diagnosis_date AS earliest_diagnosis_date': 'earliest_diagnosis_date',
        'latest_af_diagnosis_date AS latest_diagnosis_date': 'latest_diagnosis_date',
        
        'earliest_asthma_diagnosis_date AS earliest_diagnosis_date': 'earliest_diagnosis_date',
        'latest_asthma_diagnosis_date AS latest_diagnosis_date': 'latest_diagnosis_date',
        
        'earliest_cancer_diagnosis_date AS earliest_diagnosis_date': 'earliest_diagnosis_date',
        'latest_cancer_diagnosis_date AS latest_diagnosis_date': 'latest_diagnosis_date',
        
        'earliest_chd_date AS earliest_diagnosis_date': 'earliest_diagnosis_date',
        'latest_chd_date AS latest_diagnosis_date': 'latest_diagnosis_date',
        
        'earliest_ckd_diagnosis_date AS earliest_diagnosis_date': 'earliest_diagnosis_date',
        'latest_ckd_diagnosis_date AS latest_diagnosis_date': 'latest_diagnosis_date',
        
        'earliest_diagnosis_date': 'earliest_diagnosis_date',  # COPD already uses standard names
        'latest_diagnosis_date': 'latest_diagnosis_date',
        
        'earliest_dementia_diagnosis_date AS earliest_diagnosis_date': 'earliest_diagnosis_date',
        'latest_dementia_diagnosis_date AS latest_diagnosis_date': 'latest_diagnosis_date',
        
        'earliest_depression_diagnosis_date AS earliest_diagnosis_date': 'earliest_diagnosis_date',
        'latest_depression_diagnosis_date AS latest_diagnosis_date': 'latest_diagnosis_date',
        
        'earliest_diabetes_date AS earliest_diagnosis_date': 'earliest_diagnosis_date',
        'latest_diabetes_date AS latest_diagnosis_date': 'latest_diagnosis_date',
        
        'earliest_epilepsy_diagnosis_date AS earliest_diagnosis_date': 'earliest_diagnosis_date',
        'latest_epilepsy_diagnosis_date AS latest_diagnosis_date': 'latest_diagnosis_date',
        
        'earliest_fh_date AS earliest_diagnosis_date': 'earliest_diagnosis_date',
        'latest_fh_date AS latest_diagnosis_date': 'latest_diagnosis_date',
        
        'earliest_gestational_diabetes_date AS earliest_diagnosis_date': 'earliest_diagnosis_date',
        'latest_gestational_diabetes_date AS latest_diagnosis_date': 'latest_diagnosis_date',
        
        'earliest_hf_diagnosis_date AS earliest_diagnosis_date': 'earliest_diagnosis_date',
        'latest_hf_diagnosis_date AS latest_diagnosis_date': 'latest_diagnosis_date',
        
        'earliest_htn_diagnosis_date AS earliest_diagnosis_date': 'earliest_diagnosis_date',
        'latest_htn_diagnosis_date AS latest_diagnosis_date': 'latest_diagnosis_date',
        
        'earliest_ld_diagnosis_date AS earliest_diagnosis_date': 'earliest_diagnosis_date',
        'latest_ld_diagnosis_date AS latest_diagnosis_date': 'latest_diagnosis_date',
        
        'earliest_nafld_date AS earliest_diagnosis_date': 'earliest_diagnosis_date',
        'latest_nafld_date AS latest_diagnosis_date': 'latest_diagnosis_date',
        
        'earliest_any_ndh_date AS earliest_diagnosis_date': 'earliest_diagnosis_date',
        'latest_any_ndh_date AS latest_diagnosis_date': 'latest_diagnosis_date',
        
        'latest_valid_bmi_date AS earliest_diagnosis_date': 'earliest_diagnosis_date',  # Obesity special case
        'latest_bmi_date AS latest_diagnosis_date': 'latest_diagnosis_date',
        
        'earliest_osteoporosis_date AS earliest_diagnosis_date': 'earliest_diagnosis_date',
        'latest_osteoporosis_date AS latest_diagnosis_date': 'latest_diagnosis_date',
        
        'earliest_pad_date AS earliest_diagnosis_date': 'earliest_diagnosis_date',
        'latest_pad_date AS latest_diagnosis_date': 'latest_diagnosis_date',
        
        'earliest_palliative_care_date AS earliest_diagnosis_date': 'earliest_diagnosis_date',
        'latest_palliative_care_date AS latest_diagnosis_date': 'latest_diagnosis_date',
        
        'earliest_ra_date AS earliest_diagnosis_date': 'earliest_diagnosis_date',
        'latest_ra_date AS latest_diagnosis_date': 'latest_diagnosis_date',
        
        'earliest_smi_diagnosis_date AS earliest_diagnosis_date': 'earliest_diagnosis_date',
        'latest_smi_diagnosis_date AS latest_diagnosis_date': 'latest_diagnosis_date',
        
        'earliest_stroke_tia_date AS earliest_diagnosis_date': 'earliest_diagnosis_date',
        'latest_stroke_tia_date AS latest_diagnosis_date': 'latest_diagnosis_date',
    }
    
    original_content = content
    changes = 0
    
    # Apply replacements to the LTC summary
    for old_pattern, new_pattern in replacements.items():
        if old_pattern in content:
            content = content.replace(old_pattern, new_pattern)
            changes += 1
            print(f"  - Updated LTC summary: '{old_pattern}' -> '{new_pattern}'")
    
    # Write back if changes were made
    if content != original_content:
        with open(ltc_file, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"  ✓ Updated {ltc_file} ({changes} changes)")
        return True
    else:
        print(f"  - No changes needed for {ltc_file}")
        return False

def main():
    """Main function to run the standardisation"""
    print("🔄 Standardising disease register column names...")
    print("=" * 60)
    
    # Step 1: Standardise register files
    print("\n📋 Step 1: Updating individual register models")
    register_changes = standardise_register_files()
    
    # Step 2: Update LTC summary
    print("\n📊 Step 2: Updating LTC summary model")
    ltc_updated = update_ltc_summary()
    
    # Summary
    print("\n" + "=" * 60)
    print("✅ Standardisation complete!")
    
    if register_changes:
        print(f"\n📁 Register files updated: {len(register_changes)}")
        for file_path, changes in register_changes.items():
            print(f"  - {file_path}: {changes} changes")
    else:
        print("\n📁 No register files needed updates")
    
    if ltc_updated:
        print("\n📊 LTC summary model updated")
    else:
        print("\n📊 LTC summary model was already up to date")
    
    print("\n🎯 All models now use standardised column names:")
    print("  - earliest_diagnosis_date")
    print("  - latest_diagnosis_date")

if __name__ == "__main__":
    main() 