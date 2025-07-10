#!/usr/bin/env python3
"""
Script to update SQL files to use generate_table_comment macro instead of inline post_hook comments.

This script:
1. Finds all SQL files with existing post_hook comments
2. Replaces inline post_hook comments with macro-based approach
3. Preserves other post_hook configurations if they exist
"""

import os
import re
from pathlib import Path
from typing import Optional, Tuple

def extract_and_replace_posthook(sql_content: str) -> Tuple[str, bool]:
    """
    Replace inline post_hook comment with macro-based approach.
    
    Args:
        sql_content (str): Content of the SQL file
        
    Returns:
        Tuple[str, bool]: (updated_content, was_modified)
    """
    # Pattern to match the entire post_hook comment block
    pattern = r'post_hook\s*=\s*\[\s*["\']COMMENT ON TABLE \{\{ this \}\} IS ["\'][^"\']*(?:[^"\'\\]|\\.)*["\']["\']?\s*\]'
    
    match = re.search(pattern, sql_content, re.DOTALL | re.IGNORECASE)
    if match:
        # Replace with macro-based approach
        replacement = "post_hook=\"COMMENT ON TABLE {{ this }} IS '{{ generate_table_comment() }}'\""
        
        updated_content = re.sub(pattern, replacement, sql_content, flags=re.DOTALL | re.IGNORECASE)
        return updated_content, True
    
    return sql_content, False

def process_sql_file(sql_path: Path) -> Tuple[bool, bool]:
    """
    Process a single SQL file to replace post_hook comments.
    
    Args:
        sql_path (Path): Path to the SQL file
        
    Returns:
        Tuple[bool, bool]: (success, was_modified)
    """
    try:
        with open(sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        updated_content, was_modified = extract_and_replace_posthook(sql_content)
        
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
            # Find all .sql files recursively
            sql_files.extend(search_path.rglob('*.sql'))
    
    return sorted(sql_files)

def main():
    """Main function to process all SQL files and update post_hook usage."""
    print("🔄 Starting update of SQL files to use generate_table_comment macro...")
    
    # Get project root directory
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    
    print(f"📁 Project root: {project_root}")
    
    # Find all SQL files to process
    sql_files = find_sql_files(project_root)
    print(f"🔍 Found {len(sql_files)} SQL files to process")
    
    # Track statistics
    processed_count = 0
    updated_count = 0
    skipped_count = 0
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
            print("  ✅ Updated to use generate_table_comment macro")
            updated_count += 1
        else:
            print("  ⏭️  No post_hook comment found, skipping")
            skipped_count += 1
    
    # Print summary
    print(f"\n📊 Update Summary:")
    print(f"   Processed: {processed_count} files")
    print(f"   Updated:   {updated_count} files")
    print(f"   Skipped:   {skipped_count} files (no post_hook)")
    print(f"   Errors:    {error_count} files")
    
    if updated_count > 0:
        print(f"\n✨ Successfully updated {updated_count} SQL files to use the macro approach!")
        print("💡 Next steps:")
        print("   1. Test a few models to ensure the macro works correctly")
        print("   2. Run dbt compile to check for any syntax issues")
        print("   3. Fix any YAML escaping issues in schema files")
        print("   4. Test that table comments are generated correctly")
    else:
        print("\n🎯 No files were updated. Check that SQL files contain post_hook comments.")

if __name__ == "__main__":
    main()