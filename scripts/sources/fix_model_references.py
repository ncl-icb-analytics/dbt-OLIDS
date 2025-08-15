#!/usr/bin/env python3
"""
Fix model references in staging and downstream models.
Updates source() and ref() calls to use new naming conventions.
"""

import os
import re
from pathlib import Path

# Path configuration
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = Path(CURRENT_DIR).parent.parent

# Define reference mappings
SOURCE_MAPPINGS = {
    # Old source name -> New source name
    'OLIDS_TERMINOLOGY': 'olids_terminology',
    'Dictionary_dbo': 'dictionary',
    'Dictionary.dbo': 'dictionary',
}

MODEL_MAPPINGS = {
    # Old model names -> New model names
    'stg_dictionary_organisation_descendent': 'stg_dictionary_organisationdescendent',
    'stg_dictionary_organisation_matrix_practice_view': 'stg_dictionary_organisationmatrixpracticeview',
}

def fix_source_references(file_path):
    """Fix source() references in a model file."""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        original_content = content
        
        # Fix source references
        for old_source, new_source in SOURCE_MAPPINGS.items():
            # Pattern: source('OLD_SOURCE', 'TABLE_NAME')
            pattern = rf"source\(\s*['\"]({re.escape(old_source)})['\"],\s*['\"]([^'\"]+)['\"]\s*\)"
            replacement = rf"source('{new_source}', '\2')"
            content = re.sub(pattern, replacement, content)
        
        # Write back if changed
        if content != original_content:
            with open(file_path, 'w') as f:
                f.write(content)
            return True
        
        return False
        
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def fix_model_references(file_path):
    """Fix ref() references in a model file."""
    try:
        with open(file_path, 'r') as f:
            content = f.read()
        
        original_content = content
        
        # Fix model references
        for old_model, new_model in MODEL_MAPPINGS.items():
            # Pattern: ref('OLD_MODEL')
            pattern = rf"ref\(\s*['\"]({re.escape(old_model)})['\"]s*\)"
            replacement = rf"ref('{new_model}')"
            content = re.sub(pattern, replacement, content)
        
        # Write back if changed
        if content != original_content:
            with open(file_path, 'w') as f:
                f.write(content)
            return True
        
        return False
        
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return False

def process_sql_files(directory):
    """Process all SQL files in a directory."""
    source_fixes = 0
    model_fixes = 0
    
    for sql_file in Path(directory).rglob('*.sql'):
        print(f"Processing: {sql_file.relative_to(PROJECT_DIR)}")
        
        if fix_source_references(sql_file):
            source_fixes += 1
            print(f"  ✓ Fixed source references")
        
        if fix_model_references(sql_file):
            model_fixes += 1
            print(f"  ✓ Fixed model references")
    
    return source_fixes, model_fixes

def main():
    """Main function."""
    print("Fixing model and source references...")
    print("=" * 50)
    
    # Process models directory
    models_dir = PROJECT_DIR / 'models'
    source_fixes, model_fixes = process_sql_files(models_dir)
    
    print("\n" + "=" * 50)
    print("REFERENCE FIXING COMPLETE")
    print("=" * 50)
    print(f"✓ Source reference fixes: {source_fixes}")
    print(f"✓ Model reference fixes: {model_fixes}")
    
    if source_fixes + model_fixes > 0:
        print(f"\nNext steps:")
        print(f"1. Review the updated files")
        print(f"2. Test the models: dbt run --select staging")
    else:
        print(f"\nNo references needed fixing!")

if __name__ == '__main__':
    main()