#!/usr/bin/env python3
"""
Fix YAML description spacing across all dbt YAML files.

This script ensures consistent spacing in YAML description blocks to match the good format:
- Headers (Clinical Purpose:, Data Granularity:, Key Features:) should have a blank line after them
- Each bullet point should have a blank line before it

Usage:
    python fix_yaml_description_spacing.py [directory]
    
Arguments:
    directory: Root directory to process (default: models)
"""

import os
import sys
import argparse
import re

def fix_description_spacing(content):
    """
    Fix spacing in YAML description blocks.
    
    Returns:
        str: Content with fixed spacing
    """
    lines = content.split('\n')
    fixed_lines = []
    in_description = False
    
    # Target headers we want to fix
    target_headers = ['Clinical Purpose:', 'Data Granularity:', 'Key Features:']
    
    i = 0
    while i < len(lines):
        line = lines[i]
        
        # Check if we're starting a description block
        if re.match(r'\s*description:\s*[\'"]', line) and not in_description:
            in_description = True
            fixed_lines.append(line)
            i += 1
            continue
            
        # Check if we're ending a description block (closing quote)
        if in_description and (line.strip().endswith('"') or line.strip().endswith("'")):
            # This is the last line of description
            fixed_lines.append(line)
            in_description = False
            i += 1
            continue
            
        # If we're in a description block, process the content
        if in_description:
            stripped = line.strip()
            
            # Check if this line is one of our target headers
            is_target_header = any(stripped == header for header in target_headers)
            
            # Check if this is a bullet point
            is_bullet = stripped.startswith('•') or stripped.startswith('-') or stripped.startswith('*')
            
            if is_target_header:
                # Add the header line
                fixed_lines.append(line)
                
                # Check if the next line is empty - if not, we need to add a blank line
                if i + 1 < len(lines):
                    next_line = lines[i + 1]
                    if next_line.strip():  # Next line has content
                        # Add a blank line after the header
                        fixed_lines.append('')
                else:
                    # This is the last line, add a blank line anyway
                    fixed_lines.append('')
                    
            elif is_bullet:
                # Check if previous line is empty - if not, we need to add a blank line before bullet
                if fixed_lines and fixed_lines[-1].strip():
                    # Previous line has content, add blank line before bullet
                    fixed_lines.append('')
                
                # Add the bullet point
                fixed_lines.append(line)
                
            else:
                # Regular content line
                fixed_lines.append(line)
        else:
            # Not in description, just copy the line
            fixed_lines.append(line)
            
        i += 1
    
    return '\n'.join(fixed_lines)

def process_yaml_files(directory):
    """
    Process all YAML files in directory and subdirectories.
    
    Args:
        directory (str): Directory path to process
        
    Returns:
        tuple: (total_files_found, files_processed)
    """
    yaml_files = []
    
    # Find all .yml files
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.yml'):
                yaml_files.append(os.path.join(root, file))
    
    print(f"Found {len(yaml_files)} YAML files to process in {directory}")
    
    processed = 0
    for yaml_file in yaml_files:
        try:
            with open(yaml_file, 'r', encoding='utf-8') as f:
                original_content = f.read()
            
            # Apply spacing fixes
            fixed_content = fix_description_spacing(original_content)
            
            # Only write if content changed
            if fixed_content != original_content:
                with open(yaml_file, 'w', encoding='utf-8') as f:
                    f.write(fixed_content)
                processed += 1
                print(f"Fixed spacing in: {yaml_file}")
        
        except Exception as e:
            print(f"Error processing {yaml_file}: {e}")
    
    return len(yaml_files), processed

def main():
    """Main function with argument parsing."""
    parser = argparse.ArgumentParser(
        description="Fix YAML description spacing in dbt project files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                              # Process models/ directory
  %(prog)s models/marts                 # Process specific subdirectory
        """
    )
    
    parser.add_argument(
        'directory',
        nargs='?',
        default='models',
        help='Directory to process (default: models)'
    )
    
    args = parser.parse_args()
    
    # Validate directory exists
    if not os.path.exists(args.directory):
        print(f"Error: Directory '{args.directory}' not found")
        sys.exit(1)
    
    # Process files
    total_files, processed_files = process_yaml_files(args.directory)
    
    print(f"\nSummary:")
    print(f"  Total YAML files found: {total_files}")
    print(f"  Files with spacing fixes: {processed_files}")
    print(f"  Files unchanged: {total_files - processed_files}")
    
    if processed_files > 0:
        print(f"\n✅ Successfully fixed spacing in {processed_files} files")
    else:
        print(f"\n✅ All files already have correct spacing")

if __name__ == "__main__":
    main()