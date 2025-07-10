#!/usr/bin/env python3
"""
Script to remove post_hook configurations from individual SQL files 
since they're now centralized in dbt_project.yml.

This script:
1. Finds all SQL files with post_hook configurations
2. Removes the post_hook lines from the config blocks
3. Cleans up any empty config blocks if post_hook was the only configuration
"""

import re
from pathlib import Path
from typing import Tuple

def remove_post_hook_from_sql(sql_content: str) -> Tuple[str, bool]:
    """
    Remove post_hook configuration from SQL file content.
    
    Args:
        sql_content (str): Content of the SQL file
        
    Returns:
        Tuple[str, bool]: (updated_content, was_modified)
    """
    # Pattern to match post_hook lines (with various quote styles)
    post_hook_pattern = r'\s*post_hook\s*=\s*["\']COMMENT ON TABLE \{\{ this \}\} IS ["\'][^"\']*(?:[^"\'\\]|\\.)*["\']["\']?,?\s*\n?'
    
    # Check if post_hook exists
    if not re.search(post_hook_pattern, sql_content, re.DOTALL | re.IGNORECASE):
        return sql_content, False
    
    # Remove the post_hook line
    updated_content = re.sub(post_hook_pattern, '', sql_content, flags=re.DOTALL | re.IGNORECASE)
    
    # Clean up any resulting empty config blocks or trailing commas
    # Pattern for config blocks that might now be empty or have trailing commas
    config_pattern = r'(\{\{\s*config\(\s*)((?:\s*,\s*)*)(.*?)(\s*\)\s*\}\})'
    
    def clean_config_block(match):
        prefix = match.group(1)
        leading_commas = match.group(2) 
        content = match.group(3)
        suffix = match.group(4)
        
        # Remove leading/trailing commas and whitespace
        content = content.strip().strip(',').strip()
        
        # If content is empty, return empty config
        if not content:
            return "{{ config() }}"
        
        return f"{prefix}{content}{suffix}"
    
    updated_content = re.sub(config_pattern, clean_config_block, updated_content, flags=re.DOTALL)
    
    return updated_content, True

def process_sql_file(sql_path: Path) -> Tuple[bool, bool]:
    """
    Process a single SQL file to remove post_hook.
    
    Args:
        sql_path (Path): Path to the SQL file
        
    Returns:
        Tuple[bool, bool]: (success, was_modified)
    """
    try:
        with open(sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        updated_content, was_modified = remove_post_hook_from_sql(sql_content)
        
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
    """Main function to remove post_hook from all SQL files."""
    print("🔄 Removing individual post_hook configurations...")
    
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
        print(f"\n📄 Processing: {relative_path}")
        
        # Process the SQL file
        success, was_modified = process_sql_file(sql_path)
        
        if not success:
            error_count += 1
            continue
        
        processed_count += 1
        
        if was_modified:
            print("  ✅ Removed post_hook configuration")
            updated_count += 1
        else:
            print("  ℹ️  No post_hook found")
    
    # Print summary
    print(f"\n📊 Post-hook Removal Summary:")
    print(f"   Processed: {processed_count} files")
    print(f"   Updated:   {updated_count} files")
    print(f"   Errors:    {error_count} files")
    
    if updated_count > 0:
        print(f"\n✨ Successfully removed post_hook from {updated_count} SQL files!")
        print("💡 Post-hook is now centralized in dbt_project.yml")
    else:
        print("\n🎯 No post_hook configurations found in SQL files.")

if __name__ == "__main__":
    main()