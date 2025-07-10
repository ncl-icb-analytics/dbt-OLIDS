#!/usr/bin/env python3
"""
Script to fix YAML escaping issues in schema descriptions.

This script:
1. Finds all .yml files with unescaped single quotes in descriptions
2. Properly escapes single quotes by doubling them ('') 
3. Ensures proper YAML formatting for multi-line descriptions
"""

import yaml
import re
from pathlib import Path
from typing import Dict, List, Tuple

def fix_description_escaping(description: str) -> str:
    """
    Fix escaping issues in a description string.
    
    Args:
        description (str): Original description with potential escaping issues
        
    Returns:
        str: Fixed description with proper escaping
    """
    if not description:
        return description
    
    # Replace single quotes with doubled single quotes for YAML escaping
    fixed_description = description.replace("'", "''")
    
    return fixed_description

def process_yaml_file(file_path: Path) -> Tuple[bool, int]:
    """
    Process a single YAML file to fix escaping issues.
    
    Args:
        file_path (Path): Path to the YAML file
        
    Returns:
        Tuple[bool, int]: (success, number_of_fixes)
    """
    try:
        # Read the file
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Parse YAML
        data = yaml.safe_load(content)
        if not data:
            return True, 0
        
        fixes_made = 0
        
        # Process models section
        models = data.get('models', [])
        for model in models:
            if 'description' in model:
                original_desc = model['description']
                fixed_desc = fix_description_escaping(original_desc)
                
                if original_desc != fixed_desc:
                    model['description'] = fixed_desc
                    fixes_made += 1
                    print(f"    Fixed description for model: {model.get('name', 'unnamed')}")
        
        # Only rewrite file if fixes were made
        if fixes_made > 0:
            # Write back with proper YAML formatting
            with open(file_path, 'w', encoding='utf-8') as f:
                yaml.dump(data, f, default_flow_style=False, sort_keys=False,
                         allow_unicode=True, width=1000)
        
        return True, fixes_made
    
    except Exception as e:
        print(f"    Error processing {file_path}: {e}")
        return False, 0

def find_yaml_files_with_issues(base_path: Path) -> List[Path]:
    """
    Find YAML files that might have escaping issues.
    
    Args:
        base_path (Path): Base path to search
        
    Returns:
        List[Path]: List of YAML files to check
    """
    yaml_files = []
    
    # Search in models directory for .yml files
    models_path = base_path / 'models'
    if models_path.exists():
        yaml_files.extend(models_path.rglob('*.yml'))
    
    return sorted(yaml_files)

def check_for_escaping_issues(file_path: Path) -> bool:
    """
    Check if a file has potential escaping issues.
    
    Args:
        file_path (Path): Path to check
        
    Returns:
        bool: True if issues found, False otherwise
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Check for patterns that indicate escaping issues
        # Look for single quotes in description content that aren't properly escaped
        if re.search(r"description:\s*'[^']*'[^']*'", content):
            return True
        
        # Look for unescaped single quotes in multi-line descriptions
        if re.search(r"description:\s*'[^']*\n[^']*'[^']*\n", content):
            return True
        
        return False
    
    except Exception:
        return False

def main():
    """Main function to fix YAML escaping issues."""
    print("🔄 Starting YAML escaping fix...")
    
    # Get project root directory
    project_root = Path('/mnt/c/Projects/snowflake-hei-migration-dbt')
    
    print(f"📁 Project root: {project_root}")
    
    # Find all YAML files
    yaml_files = find_yaml_files_with_issues(project_root)
    print(f"🔍 Found {len(yaml_files)} YAML files to check")
    
    # Track statistics
    checked_count = 0
    fixed_files = 0
    total_fixes = 0
    error_count = 0
    
    for yaml_path in yaml_files:
        relative_path = yaml_path.relative_to(project_root)
        
        # Skip certain files that might not need fixing
        if any(skip in str(relative_path) for skip in ['sources.yml', 'schema.yml']):
            continue
        
        # Check if file has potential issues
        has_issues = check_for_escaping_issues(yaml_path)
        
        if has_issues:
            print(f"\n📄 Processing: {relative_path}")
            
            # Process the file
            success, fixes = process_yaml_file(yaml_path)
            
            if success:
                checked_count += 1
                if fixes > 0:
                    fixed_files += 1
                    total_fixes += fixes
                    print(f"  ✅ Fixed {fixes} description(s)")
                else:
                    print(f"  ℹ️  No fixes needed")
            else:
                error_count += 1
        else:
            checked_count += 1
    
    # Print summary
    print(f"\n📊 YAML Escaping Fix Summary:")
    print(f"   Checked:     {checked_count} files")
    print(f"   Fixed files: {fixed_files} files")
    print(f"   Total fixes: {total_fixes} descriptions")
    print(f"   Errors:      {error_count} files")
    
    if total_fixes > 0:
        print(f"\n✨ Successfully fixed {total_fixes} YAML escaping issues!")
        print("💡 Next step: Test the macro approach with updated schema files")
    else:
        print("\n🎯 No YAML escaping issues found or all were already fixed.")

if __name__ == "__main__":
    main()