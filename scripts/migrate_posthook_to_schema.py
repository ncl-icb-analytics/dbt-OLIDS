#!/usr/bin/env python3
"""
Script to migrate post_hook comments from SQL files to schema.yml descriptions.

This script:
1. Scans intermediate and marts directories for SQL files (skips staging)
2. Extracts post_hook comment content from each SQL file
3. Creates or updates corresponding .yml files with the extracted descriptions
4. Preserves existing schema structure and tests
"""

import os
import re
import yaml
from pathlib import Path
from typing import Dict, Optional, Tuple

def extract_posthook_comment(sql_content: str) -> Optional[str]:
    """
    Extract the comment content from post_hook configuration.
    
    Args:
        sql_content (str): Content of the SQL file
        
    Returns:
        Optional[str]: Extracted comment content, or None if no post_hook found
    """
    # Pattern to match post_hook comment
    # Matches: post_hook=[ "COMMENT ON TABLE {{ this }} IS 'content'" ]
    pattern = r'post_hook\s*=\s*\[\s*["\']COMMENT ON TABLE \{\{ this \}\} IS ["\']([^"\']*(?:[^"\'\\]|\\.)*)["\']["\']?\s*\]'
    
    match = re.search(pattern, sql_content, re.DOTALL | re.IGNORECASE)
    if match:
        comment_content = match.group(1)
        # Clean up the content - remove extra whitespace and normalize line breaks
        comment_content = re.sub(r'\n\s+', '\n', comment_content)
        comment_content = comment_content.strip()
        return comment_content
    
    return None

def load_existing_schema(schema_path: Path) -> Dict:
    """
    Load existing schema.yml file if it exists.
    
    Args:
        schema_path (Path): Path to the schema.yml file
        
    Returns:
        Dict: Parsed YAML content or default structure
    """
    if schema_path.exists():
        try:
            with open(schema_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            print(f"Warning: Could not parse existing schema {schema_path}: {e}")
    
    # Return default structure
    return {
        'version': 2,
        'models': []
    }

def find_model_in_schema(schema_data: Dict, model_name: str) -> Optional[Dict]:
    """
    Find a specific model in the schema data.
    
    Args:
        schema_data (Dict): Parsed schema YAML data
        model_name (str): Name of the model to find
        
    Returns:
        Optional[Dict]: Model entry if found, None otherwise
    """
    models = schema_data.get('models', [])
    for model in models:
        if model.get('name') == model_name:
            return model
    return None

def update_model_description(schema_data: Dict, model_name: str, description: str) -> Dict:
    """
    Update or add a model description in schema data.
    
    Args:
        schema_data (Dict): Parsed schema YAML data
        model_name (str): Name of the model
        description (str): Description to set
        
    Returns:
        Dict: Updated schema data
    """
    # Ensure models list exists
    if 'models' not in schema_data:
        schema_data['models'] = []
    
    # Find existing model or create new one
    existing_model = find_model_in_schema(schema_data, model_name)
    
    if existing_model:
        # Update existing model
        existing_description = existing_model.get('description', '')
        if existing_description and existing_description != description:
            print(f"  Warning: Model {model_name} already has description, overwriting")
        existing_model['description'] = description
    else:
        # Create new model entry
        new_model = {
            'name': model_name,
            'description': description
        }
        schema_data['models'].append(new_model)
    
    return schema_data

def save_schema_file(schema_path: Path, schema_data: Dict):
    """
    Save schema data to YAML file with proper formatting.
    
    Args:
        schema_path (Path): Path to save the schema file
        schema_data (Dict): Schema data to save
    """
    # Ensure directory exists
    schema_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(schema_path, 'w', encoding='utf-8') as f:
        yaml.dump(schema_data, f, default_flow_style=False, sort_keys=False, 
                 allow_unicode=True, width=1000)

def process_sql_file(sql_path: Path) -> Tuple[bool, Optional[str]]:
    """
    Process a single SQL file to extract post_hook comment.
    
    Args:
        sql_path (Path): Path to the SQL file
        
    Returns:
        Tuple[bool, Optional[str]]: (success, extracted_comment)
    """
    try:
        with open(sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        comment = extract_posthook_comment(sql_content)
        return True, comment
    
    except Exception as e:
        print(f"Error reading {sql_path}: {e}")
        return False, None

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
    """Main function to process all SQL files and migrate comments."""
    print("🔄 Starting migration of post_hook comments to schema descriptions...")
    
    # Get project root directory
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    
    print(f"📁 Project root: {project_root}")
    
    # Find all SQL files to process
    sql_files = find_sql_files(project_root)
    print(f"🔍 Found {len(sql_files)} SQL files to process")
    
    # Track statistics
    processed_count = 0
    migrated_count = 0
    skipped_count = 0
    error_count = 0
    
    for sql_path in sql_files:
        relative_path = sql_path.relative_to(project_root)
        print(f"\n📄 Processing: {relative_path}")
        
        # Extract comment from SQL file
        success, comment = process_sql_file(sql_path)
        
        if not success:
            error_count += 1
            continue
        
        processed_count += 1
        
        if not comment:
            print("  ⏭️  No post_hook comment found, skipping")
            skipped_count += 1
            continue
        
        # Determine schema file path (same directory, same name but .yml)
        model_name = sql_path.stem
        schema_path = sql_path.parent / f"{model_name}.yml"
        
        print(f"  📝 Extracting comment ({len(comment)} chars)")
        print(f"  📋 Target schema: {schema_path.relative_to(project_root)}")
        
        try:
            # Load existing schema or create new one
            schema_data = load_existing_schema(schema_path)
            
            # Update with extracted description
            schema_data = update_model_description(schema_data, model_name, comment)
            
            # Save updated schema file
            save_schema_file(schema_path, schema_data)
            
            print("  ✅ Successfully migrated comment to schema description")
            migrated_count += 1
            
        except Exception as e:
            print(f"  ❌ Error updating schema: {e}")
            error_count += 1
    
    # Print summary
    print(f"\n📊 Migration Summary:")
    print(f"   Processed: {processed_count} files")
    print(f"   Migrated:  {migrated_count} comments")
    print(f"   Skipped:   {skipped_count} files (no comments)")
    print(f"   Errors:    {error_count} files")
    
    if migrated_count > 0:
        print(f"\n✨ Successfully migrated {migrated_count} post_hook comments to schema descriptions!")
        print("💡 Next steps:")
        print("   1. Review the generated .yml files")
        print("   2. Test that descriptions appear correctly in dbt docs")
        print("   3. Create macro to generate post_hooks from descriptions")
        print("   4. Update models to use the new macro approach")
    else:
        print("\n🎯 No comments were migrated. Check that SQL files contain post_hook comments.")

if __name__ == "__main__":
    main()