#!/usr/bin/env python3
"""
Script to fix flu schema duplicates by merging rich descriptions into consolidated files
and removing the duplicate individual .yml files.
"""

import yaml
from pathlib import Path
from typing import Dict, List

def load_yaml_file(file_path: Path) -> Dict:
    """Load and parse a YAML file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return {}

def save_yaml_file(file_path: Path, data: Dict):
    """Save data to a YAML file with proper formatting."""
    with open(file_path, 'w', encoding='utf-8') as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False, 
                 allow_unicode=True, width=1000)

def extract_description_from_individual_file(file_path: Path) -> str:
    """Extract the description from an individual model .yml file."""
    data = load_yaml_file(file_path)
    models = data.get('models', [])
    if models and len(models) > 0:
        return models[0].get('description', '')
    return ''

def update_model_in_consolidated_file(consolidated_data: Dict, model_name: str, new_description: str) -> bool:
    """Update a model's description in the consolidated schema data."""
    models = consolidated_data.get('models', [])
    for model in models:
        if model.get('name') == model_name:
            # Store old description for comparison
            old_desc = model.get('description', '')
            if old_desc != new_description and new_description:
                print(f"  Updating description for {model_name}")
                print(f"    Old: {old_desc[:100]}{'...' if len(old_desc) > 100 else ''}")
                print(f"    New: {new_description[:100]}{'...' if len(new_description) > 100 else ''}")
                model['description'] = new_description
                return True
            else:
                print(f"  No change needed for {model_name}")
    return False

def main():
    """Main function to fix flu schema duplicates."""
    print("🔄 Fixing flu schema duplicates...")
    
    project_root = Path('/mnt/c/Projects/snowflake-hei-migration-dbt')
    
    # Define the mappings
    mappings = [
        {
            'consolidated_file': 'models/intermediate/programme/flu/flu_intermediate_schema.yml',
            'individual_pattern': 'models/intermediate/programme/flu/int_flu_*.yml'
        },
        {
            'consolidated_file': 'models/marts/programme/flu/flu_marts_schema.yml',
            'individual_pattern': 'models/marts/programme/flu/fct_flu_*.yml'
        }
    ]
    
    for mapping in mappings:
        consolidated_path = project_root / mapping['consolidated_file']
        individual_files = list(project_root.glob(mapping['individual_pattern']))
        
        print(f"\n📁 Processing {mapping['consolidated_file']}")
        print(f"   Found {len(individual_files)} individual files to merge")
        
        if not consolidated_path.exists():
            print(f"   ❌ Consolidated file not found: {consolidated_path}")
            continue
        
        # Load consolidated file
        consolidated_data = load_yaml_file(consolidated_path)
        if not consolidated_data:
            print(f"   ❌ Could not load consolidated file")
            continue
        
        # Track changes
        changes_made = False
        files_to_remove = []
        
        # Process each individual file
        for individual_file in individual_files:
            model_name = individual_file.stem  # filename without extension
            print(f"\n  📄 Processing {model_name}")
            
            # Extract rich description from individual file
            rich_description = extract_description_from_individual_file(individual_file)
            
            if rich_description:
                # Update in consolidated file
                updated = update_model_in_consolidated_file(consolidated_data, model_name, rich_description)
                if updated:
                    changes_made = True
                
                # Mark for removal
                files_to_remove.append(individual_file)
            else:
                print(f"    ⚠️  No description found in {individual_file}")
        
        # Save updated consolidated file if changes were made
        if changes_made:
            print(f"\n  💾 Saving updated consolidated file")
            save_yaml_file(consolidated_path, consolidated_data)
        
        # Remove individual files
        print(f"\n  🗑️  Removing {len(files_to_remove)} duplicate files")
        for file_to_remove in files_to_remove:
            try:
                file_to_remove.unlink()
                print(f"    ✅ Removed {file_to_remove.name}")
            except Exception as e:
                print(f"    ❌ Error removing {file_to_remove.name}: {e}")
    
    print(f"\n✨ Flu schema duplicate fix complete!")
    print("💡 Next step: Fix YAML escaping issues in descriptions")

if __name__ == "__main__":
    main()