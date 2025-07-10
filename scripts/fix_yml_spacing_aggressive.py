#!/usr/bin/env python3
"""
Script to aggressively clean up YAML line spacing.

This script:
1. Removes ALL empty lines from descriptions
2. Adds back ONLY one empty line before lines containing colons (section headers)
3. Results in clean, compact formatting
"""

import yaml
import re
from pathlib import Path
from typing import Dict, List

def clean_description_spacing(description: str) -> str:
    """
    Aggressively clean description spacing.
    
    Args:
        description (str): Original description
        
    Returns:
        str: Description with clean spacing
    """
    if not description:
        return description
    
    # Split into lines and clean each line
    lines = description.split('\n')
    
    # Remove all empty/whitespace-only lines and strip trailing whitespace
    content_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped:  # Only keep lines with actual content
            content_lines.append(stripped)
    
    # Now rebuild with proper spacing
    result_lines = []
    
    for i, line in enumerate(content_lines):
        # Add empty line before lines with colons (section headers), but NEVER for the first line
        if i > 0 and ':' in line:
            result_lines.append('')
        
        result_lines.append(line)
    
    return '\n'.join(result_lines)

def process_yaml_file(file_path: Path) -> bool:
    """
    Process a single YAML file to fix spacing issues.
    
    Args:
        file_path (Path): Path to the YAML file
        
    Returns:
        bool: True if file was modified, False otherwise
    """
    try:
        # Read the file
        with open(file_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        
        if not data:
            return False
        
        modified = False
        
        # Process models section
        models = data.get('models', [])
        for model in models:
            if 'description' in model:
                original_desc = model['description']
                cleaned_desc = clean_description_spacing(original_desc)
                
                if original_desc != cleaned_desc:
                    model['description'] = cleaned_desc
                    modified = True
                    print(f"    Cleaned spacing for model: {model.get('name', 'unnamed')}")
            
            # Process column descriptions
            columns = model.get('columns', [])
            for column in columns:
                if 'description' in column:
                    original_desc = column['description']
                    cleaned_desc = clean_description_spacing(original_desc)
                    
                    if original_desc != cleaned_desc:
                        column['description'] = cleaned_desc
                        modified = True
        
        # Save file if modified
        if modified:
            with open(file_path, 'w', encoding='utf-8') as f:
                yaml.dump(data, f, 
                         default_flow_style=False, 
                         sort_keys=False,
                         allow_unicode=True, 
                         width=1000,
                         indent=2,
                         default_style=None)
        
        return modified
    
    except Exception as e:
        print(f"    Error processing {file_path}: {e}")
        return False

def find_yaml_files(base_path: Path) -> List[Path]:
    """Find all YAML schema files to process."""
    yaml_files = []
    
    models_path = base_path / 'models'
    if models_path.exists():
        yaml_files.extend(models_path.rglob('*.yml'))
    
    return sorted(yaml_files)

def main():
    """Main function to aggressively fix YAML spacing."""
    print("🔄 Starting aggressive YAML spacing cleanup...")
    
    project_root = Path('/mnt/c/Projects/snowflake-hei-migration-dbt')
    print(f"📁 Project root: {project_root}")
    
    yaml_files = find_yaml_files(project_root)
    print(f"🔍 Found {len(yaml_files)} YAML files to process")
    
    processed_count = 0
    modified_count = 0
    error_count = 0
    
    for yaml_path in yaml_files:
        relative_path = yaml_path.relative_to(project_root)
        
        # Skip certain files
        if any(skip in str(relative_path) for skip in ['sources.yml']):
            continue
        
        print(f"\n📄 Processing: {relative_path}")
        
        try:
            modified = process_yaml_file(yaml_path)
            processed_count += 1
            
            if modified:
                modified_count += 1
                print(f"  ✅ Cleaned spacing")
            else:
                print(f"  ℹ️  Already clean")
        except Exception as e:
            print(f"  ❌ Error: {e}")
            error_count += 1
    
    print(f"\n📊 Aggressive Spacing Cleanup Summary:")
    print(f"   Processed: {processed_count} files")
    print(f"   Modified:  {modified_count} files")
    print(f"   Errors:    {error_count} files")
    
    if modified_count > 0:
        print(f"\n✨ Successfully cleaned spacing in {modified_count} YAML files!")
    else:
        print("\n🎯 All files already have clean spacing.")

if __name__ == "__main__":
    main()