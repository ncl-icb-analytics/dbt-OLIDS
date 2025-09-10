#!/usr/bin/env python3
"""
Migration script to update downstream models for Alpha database column changes.
This updates intermediate and marts models that reference staging model columns that have changed.
"""

import os
import re
import yaml
from pathlib import Path

# Column name changes from UAT to Alpha
COLUMN_MAPPINGS = {
    # Format: 'old_column_name': 'new_column_name'
    'encounter_core_concept_id': 'encounter_source_concept_id',
    'episode_type_raw_concept_id': 'episode_type_source_concept_id',
    'episode_status_raw_concept_id': 'episode_status_source_concept_id',
    'medication_order_core_concept_id': 'medication_order_source_concept_id',
    'medication_statement_core_concept_id': 'medication_statement_source_concept_id',
    'practioner_id': 'practitioner_id',  # typo fixed
    'parent_obervation_id': 'parent_observation_id',  # typo fixed
    'post_code_hash': 'postcode_hash',  # naming standardised
    'matched_nhs_numberhash': 'matched_nhs_number_hash',  # underscore added
    'procedure_core_concept_id': 'procedure_source_concept_id',
    'referal_request_type_concept_id': 'referral_request_type_concept_id',  # typo fixed
}

# Columns removed in Alpha
REMOVED_COLUMNS = {
    'lds_business_key': ['PATIENT_PERSON', 'PERSON']
}

def update_sql_file(file_path):
    """Update a SQL file with new column names."""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original_content = content
    changes_made = []
    
    # Replace column references
    for old_col, new_col in COLUMN_MAPPINGS.items():
        # Match column references in various contexts
        patterns = [
            # Column reference with dots (e.g., table.old_column)
            (r'(\b\w+\.)"?' + re.escape(old_col) + r'"?(\b)', r'\1' + new_col + r'\2'),
            # Column reference in quotes
            (r'"' + re.escape(old_col) + r'"', f'"{new_col}"'),
            # Column reference without quotes (word boundary)
            (r'\b' + re.escape(old_col) + r'\b', new_col),
        ]
        
        for pattern, replacement in patterns:
            new_content = re.sub(pattern, replacement, content, flags=re.IGNORECASE)
            if new_content != content:
                changes_made.append(f"{old_col} -> {new_col}")
                content = new_content
    
    # Check for removed columns and add comments
    for removed_col, tables in REMOVED_COLUMNS.items():
        if removed_col in content:
            print(f"  WARNING: File references removed column '{removed_col}'")
            # Add a comment about the removed column
            content = f"-- MIGRATION NOTE: Column '{removed_col}' was removed in Alpha database\n" + content
            changes_made.append(f"Added warning for removed column: {removed_col}")
    
    if content != original_content:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        return changes_made
    
    return []

def find_downstream_models(project_dir):
    """Find all intermediate and marts models."""
    models = []
    
    # Intermediate models
    intermediate_dir = project_dir / 'models' / 'olids' / 'intermediate'
    if intermediate_dir.exists():
        models.extend(intermediate_dir.glob('**/*.sql'))
    
    # Marts models
    marts_dir = project_dir / 'models' / 'olids' / 'marts'
    if marts_dir.exists():
        models.extend(marts_dir.glob('**/*.sql'))
    
    return models

def main():
    """Run the migration."""
    # Get project directory
    script_dir = Path(__file__).parent
    project_dir = script_dir.parent
    
    print("Alpha Database Migration Script")
    print("=" * 60)
    print("\nThis script will update downstream models for Alpha database column changes.")
    print("\nColumn mappings to be applied:")
    for old, new in COLUMN_MAPPINGS.items():
        print(f"  {old} -> {new}")
    
    print("\nSearching for downstream models...")
    models = find_downstream_models(project_dir)
    print(f"Found {len(models)} downstream models to check")
    
    total_updated = 0
    for model_path in models:
        relative_path = model_path.relative_to(project_dir)
        changes = update_sql_file(model_path)
        
        if changes:
            print(f"\n✓ Updated: {relative_path}")
            for change in changes:
                print(f"    - {change}")
            total_updated += 1
    
    print("\n" + "=" * 60)
    print(f"Migration complete: {total_updated} files updated")
    
    if total_updated > 0:
        print("\nNext steps:")
        print("1. Review the changes using git diff")
        print("2. Run the sources workflow to regenerate staging models:")
        print("   python scripts/sources/run_full_sources_workflow.py")
        print("3. Test compilation: dbt compile")
        print("4. Run affected models: dbt run --select staging+")

if __name__ == '__main__':
    main()