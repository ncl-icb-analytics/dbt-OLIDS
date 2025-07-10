#!/usr/bin/env python3
"""
Script to fix excessive line spacing in YAML file descriptions.

This script removes excessive blank lines and normalizes the spacing in
multi-line descriptions while preserving intended paragraph breaks.
"""

import yaml
import re
from pathlib import Path
from typing import Dict, List

def fix_line_spacing(description: str) -> str:
    """
    Fix excessive line spacing in a description.
    Ensures lines with colons have an empty line above them (unless first line).
    
    Args:
        description (str): Original description with spacing issues
        
    Returns:
        str: Description with fixed spacing
    """
    if not description:
        return description
    
    # Split into lines and process
    lines = description.split('\n')
    processed_lines = []
    
    for i, line in enumerate(lines):
        line = line.rstrip()
        
        # Check if this line contains a colon and needs an empty line above it
        if ':' in line.strip() and i > 0 and line.strip():
            # Check if the previous line in processed_lines is empty
            if processed_lines and processed_lines[-1].strip():
                # Add empty line before lines with colons (section headers)
                processed_lines.append('')
        
        processed_lines.append(line)
    
    # Join back together
    result = '\n'.join(processed_lines)
    
    # Final cleanup: remove excessive newlines at start/end
    result = result.strip()
    
    # Remove patterns of 3+ consecutive newlines and replace with 2
    result = re.sub(r'\n{3,}', '\n\n', result)
    
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
    print("🔄 Starting YAML line spacing fix...")
    
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
    print(f"\n📊 YAML Line Spacing Fix Summary:")
    print(f"   Processed: {processed_count} files")
    print(f"   Modified:  {modified_count} files")
    print(f"   Errors:    {error_count} files")
    
    if modified_count > 0:
        print(f"\n✨ Successfully fixed line spacing in {modified_count} YAML files!")
    else:
        print("\n🎯 No line spacing issues found or all were already fixed.")

if __name__ == "__main__":
    main()