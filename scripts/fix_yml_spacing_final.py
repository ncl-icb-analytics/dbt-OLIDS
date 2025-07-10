#!/usr/bin/env python3
"""
Script to completely clean up YAML spacing issues by working with raw strings.

This script:
1. Removes ALL empty lines from descriptions
2. Adds back only one empty line before section headers (lines with colons)
3. Never adds empty line before the first line
"""

import yaml
import re
from pathlib import Path
from typing import Dict, List

def clean_description_completely(description: str) -> str:
    """
    Completely clean description spacing by removing all empty lines
    and adding back only before section headers.
    
    Args:
        description (str): Original description
        
    Returns:
        str: Description with completely clean spacing
    """
    if not description:
        return description
    
    # Split into lines and strip whitespace
    lines = description.split('\n')
    
    # Keep only non-empty lines
    content_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped:  # Only lines with actual content
            content_lines.append(stripped)
    
    if not content_lines:
        return ""
    
    # Rebuild with proper spacing
    result_lines = []
    
    for i, line in enumerate(content_lines):
        # Add empty line before section headers (lines with colons)
        # BUT never before the first line
        if i > 0 and ':' in line:
            result_lines.append('')
        
        result_lines.append(line)
    
    return '\n'.join(result_lines)

def process_yaml_file(file_path: Path) -> bool:
    """
    Process a YAML file by working with both parsed and raw content.
    """
    try:
        # Read raw content first
        with open(file_path, 'r', encoding='utf-8') as f:
            raw_content = f.read()
        
        # Parse YAML to work with structure
        data = yaml.safe_load(raw_content)
        
        if not data:
            return False
        
        modified = False
        
        # Process models section
        models = data.get('models', [])
        for model in models:
            if 'description' in model:
                original_desc = model['description']
                cleaned_desc = clean_description_completely(original_desc)
                
                if original_desc != cleaned_desc:
                    model['description'] = cleaned_desc
                    modified = True
                    print(f"    Cleaned spacing for model: {model.get('name', 'unnamed')}")
            
            # Process column descriptions
            columns = model.get('columns', [])
            for column in columns:
                if 'description' in column:
                    original_desc = column['description']
                    cleaned_desc = clean_description_completely(original_desc)
                    
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

def main():
    """Main function to completely clean YAML spacing."""
    print("🔄 Starting complete YAML spacing cleanup...")
    
    project_root = Path('/mnt/c/Projects/snowflake-hei-migration-dbt')
    print(f"📁 Project root: {project_root}")
    
    # Find all YAML files
    yaml_files = []
    models_path = project_root / 'models'
    if models_path.exists():
        yaml_files.extend(models_path.rglob('*.yml'))
    
    yaml_files = sorted(yaml_files)
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
    
    print(f"\n📊 Complete Spacing Cleanup Summary:")
    print(f"   Processed: {processed_count} files")
    print(f"   Modified:  {modified_count} files")
    print(f"   Errors:    {error_count} files")
    
    if modified_count > 0:
        print(f"\n✨ Successfully cleaned spacing in {modified_count} YAML files!")
    else:
        print("\n🎯 All files already have clean spacing.")

if __name__ == "__main__":
    main()