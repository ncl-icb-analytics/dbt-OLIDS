#!/usr/bin/env python3
"""
Script to clean up YAML line spacing by removing all empty lines and adding them back only before lines with colons.

This script:
1. Removes all empty lines from descriptions
2. Adds empty lines back only before lines containing colons (section headers)
"""

import yaml
import re
from pathlib import Path
from typing import Dict, List

def fix_line_spacing(description: str) -> str:
    """
    Fix line spacing by removing all empty lines and adding them back only before colons.
    
    Args:
        description (str): Original description with spacing issues
        
    Returns:
        str: Description with clean spacing
    """
    if not description:
        return description
    
    # Split into lines and remove all empty lines first
    lines = description.split('\n')
    non_empty_lines = [line.rstrip() for line in lines if line.strip()]
    
    # Now add empty lines back before lines with colons
    processed_lines = []
    
    for i, line in enumerate(non_empty_lines):
        # Add empty line before lines with colons (except the first line)
        if ':' in line and i > 0:
            processed_lines.append('')
        
        processed_lines.append(line)
    
    # Join back together and clean up
    result = '\n'.join(processed_lines)
    result = result.strip()
    
    return result

def process_yaml_file(file_path: Path) -> bool:
    """
    Process a single YAML file to fix line spacing issues.
    
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
                fixed_desc = fix_line_spacing(original_desc)
                
                if original_desc != fixed_desc:
                    model['description'] = fixed_desc
                    modified = True
                    print(f"    Fixed line spacing for model: {model.get('name', 'unnamed')}")
            
            # Process column descriptions as well
            columns = model.get('columns', [])
            for column in columns:
                if 'description' in column:
                    original_desc = column['description']
                    fixed_desc = fix_line_spacing(original_desc)
                    
                    if original_desc != fixed_desc:
                        column['description'] = fixed_desc
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
    """
    Find all YAML schema files to process.
    
    Args:
        base_path (Path): Base path to search
        
    Returns:
        List[Path]: List of YAML files to process
    """
    yaml_files = []
    
    # Search in models directory for .yml files
    models_path = base_path / 'models'
    if models_path.exists():
        yaml_files.extend(models_path.rglob('*.yml'))
    
    return sorted(yaml_files)

def main():
    """Main function to fix YAML line spacing issues."""
    print("🔄 Starting clean YAML line spacing fix...")
    
    # Get project root directory
    project_root = Path('/mnt/c/Projects/snowflake-hei-migration-dbt')
    
    print(f"📁 Project root: {project_root}")
    
    # Find all YAML files
    yaml_files = find_yaml_files(project_root)
    print(f"🔍 Found {len(yaml_files)} YAML files to process")
    
    # Track statistics
    processed_count = 0
    modified_count = 0
    error_count = 0
    
    for yaml_path in yaml_files:
        relative_path = yaml_path.relative_to(project_root)
        
        # Skip certain files that might not need processing
        if any(skip in str(relative_path) for skip in ['sources.yml']):
            continue
        
        print(f"\n📄 Processing: {relative_path}")
        
        # Process the file
        try:
            modified = process_yaml_file(yaml_path)
            processed_count += 1
            
            if modified:
                modified_count += 1
                print(f"  ✅ Fixed line spacing issues")
            else:
                print(f"  ℹ️  No spacing issues found")
        except Exception as e:
            print(f"  ❌ Error: {e}")
            error_count += 1
    
    # Print summary
    print(f"\n📊 Clean YAML Line Spacing Fix Summary:")
    print(f"   Processed: {processed_count} files")
    print(f"   Modified:  {modified_count} files")
    print(f"   Errors:    {error_count} files")
    
    if modified_count > 0:
        print(f"\n✨ Successfully cleaned line spacing in {modified_count} YAML files!")
    else:
        print("\n🎯 No line spacing issues found or all were already fixed.")

if __name__ == "__main__":
    main()