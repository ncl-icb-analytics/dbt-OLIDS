#!/usr/bin/env python3
"""
Script to fix data type mismatches in sources.yml based on type-mismatch-warnings.txt

This script parses the type mismatch warnings and updates the sources.yml file
to use the proper Snowflake data types that dbt is overriding to.
"""

import re
import yaml
from pathlib import Path


def parse_type_mismatch_warnings(warnings_file):
    """Parse the type mismatch warnings file and extract the type corrections."""
    type_corrections = {}
    
    with open(warnings_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Pattern to match type mismatch warnings
    pattern = r"warning: dbt1058: Column '(\w+)' in node 'source\.[\w_]+\.(\w+)\.(\w+)' has a type mismatch\. Overriding '(\w+(?:\(\d+(?:,\d+)?\))?)' with '(\w+(?:\(\d+(?:,\d+)?\))?)'."
    
    matches = re.findall(pattern, content)
    
    for match in matches:
        column_name, schema_name, table_name, old_type, new_type = match
        
        # Create nested structure: schema -> table -> column -> new_type
        if schema_name not in type_corrections:
            type_corrections[schema_name] = {}
        if table_name not in type_corrections[schema_name]:
            type_corrections[schema_name][table_name] = {}
        
        type_corrections[schema_name][table_name][column_name] = new_type
    
    return type_corrections


def update_sources_yaml(sources_file, type_corrections):
    """Update the sources.yml file with the corrected data types."""
    
    with open(sources_file, 'r', encoding='utf-8') as f:
        sources_data = yaml.safe_load(f)
    
    changes_made = 0
    
    # Iterate through sources
    for source in sources_data.get('sources', []):
        source_name = source.get('name')
        
        if source_name in type_corrections:
            # Iterate through tables in this source
            for table in source.get('tables', []):
                table_name = table.get('name')
                
                if table_name in type_corrections[source_name]:
                    # Iterate through columns in this table
                    for column in table.get('columns', []):
                        column_name = column.get('name')
                        
                        if column_name in type_corrections[source_name][table_name]:
                            old_type = column.get('data_type')
                            new_type = type_corrections[source_name][table_name][column_name]
                            
                            if old_type != new_type:
                                column['data_type'] = new_type
                                changes_made += 1
                                print(f"Updated {source_name}.{table_name}.{column_name}: {old_type} -> {new_type}")
    
    # Write back to file with proper YAML formatting
    with open(sources_file, 'w', encoding='utf-8') as f:
        yaml.dump(sources_data, f, 
                  default_flow_style=False, 
                  sort_keys=False, 
                  width=120,
                  allow_unicode=True)
    
    return changes_made


def main():
    """Main function to fix data types."""
    warnings_file = Path('type-mismatch-warnings.txt')
    sources_file = Path('models/sources.yml')
    
    if not warnings_file.exists():
        print(f"Error: {warnings_file} not found!")
        return
    
    if not sources_file.exists():
        print(f"Error: {sources_file} not found!")
        return
    
    print("Parsing type mismatch warnings...")
    type_corrections = parse_type_mismatch_warnings(warnings_file)
    
    print(f"Found corrections for {len(type_corrections)} schemas")
    for schema_name, tables in type_corrections.items():
        print(f"  {schema_name}: {len(tables)} tables")
    
    print("\nUpdating sources.yml...")
    changes_made = update_sources_yaml(sources_file, type_corrections)
    
    print(f"\nCompleted! Made {changes_made} type corrections to sources.yml")


if __name__ == "__main__":
    main() 