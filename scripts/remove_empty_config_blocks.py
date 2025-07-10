#!/usr/bin/env python3
"""
Script to remove empty config blocks from SQL files.

This script:
1. Finds SQL files with empty {{ config() }} blocks
2. Removes the empty config blocks completely
3. Cleans up any extra blank lines
"""

import re
from pathlib import Path
from typing import Tuple

def remove_empty_config(sql_content: str) -> Tuple[str, bool]:
    """
    Remove empty config blocks from SQL content.
    
    Args:
        sql_content (str): Content of the SQL file
        
    Returns:
        Tuple[str, bool]: (updated_content, was_modified)
    """
    # Pattern to match empty config blocks
    empty_config_pattern = r'\{\{\s*config\(\s*\)\s*\}\}\s*\n?'
    
    # Check if empty config exists
    if not re.search(empty_config_pattern, sql_content):
        return sql_content, False
    
    # Remove empty config blocks
    updated_content = re.sub(empty_config_pattern, '', sql_content)
    
    # Clean up any resulting multiple blank lines (more than 2)
    updated_content = re.sub(r'\n{3,}', '\n\n', updated_content)
    
    # Remove leading newlines
    updated_content = updated_content.lstrip('\n')
    
    return updated_content, True

def process_sql_file(sql_path: Path) -> Tuple[bool, bool]:
    """
    Process a single SQL file to remove empty config.
    
    Args:
        sql_path (Path): Path to the SQL file
        
    Returns:
        Tuple[bool, bool]: (success, was_modified)
    """
    try:
        with open(sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        updated_content, was_modified = remove_empty_config(sql_content)
        
        if was_modified:
            with open(sql_path, 'w', encoding='utf-8') as f:
                f.write(updated_content)
        
        return True, was_modified
    
    except Exception as e:
        print(f"Error processing {sql_path}: {e}")
        return False, False

def find_sql_files(base_path: Path) -> list[Path]:
    """
    Find all SQL files in intermediate and marts directories.
    
    Args:
        base_path (Path): Base path of the project
        
    Returns:
        list[Path]: List of SQL file paths
    """
    sql_files = []
    
    # Search in intermediate and marts directories
    for directory in ['models/intermediate', 'models/marts']:
        search_path = base_path / directory
        if search_path.exists():
            sql_files.extend(search_path.rglob('*.sql'))
    
    return sorted(sql_files)

def main():
    """Main function to remove empty config blocks from all SQL files."""
    print("🔄 Removing empty config blocks...")
    
    # Get project root directory
    project_root = Path('/mnt/c/Projects/snowflake-hei-migration-dbt')
    
    print(f"📁 Project root: {project_root}")
    
    # Find all SQL files to process
    sql_files = find_sql_files(project_root)
    print(f"🔍 Found {len(sql_files)} SQL files to process")
    
    # Track statistics
    processed_count = 0
    updated_count = 0
    error_count = 0
    
    for sql_path in sql_files:
        relative_path = sql_path.relative_to(project_root)
        
        # Process the SQL file
        success, was_modified = process_sql_file(sql_path)
        
        if not success:
            error_count += 1
            print(f"❌ Error processing: {relative_path}")
            continue
        
        processed_count += 1
        
        if was_modified:
            print(f"✅ Removed empty config from: {relative_path}")
            updated_count += 1
    
    # Print summary
    print(f"\n📊 Empty Config Removal Summary:")
    print(f"   Processed: {processed_count} files")
    print(f"   Updated:   {updated_count} files")
    print(f"   Errors:    {error_count} files")
    
    if updated_count > 0:
        print(f"\n✨ Successfully removed empty config blocks from {updated_count} SQL files!")
    else:
        print("\n🎯 No empty config blocks found.")

if __name__ == "__main__":
    main()