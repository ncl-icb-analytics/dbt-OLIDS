#!/usr/bin/env python3
"""
Fix YAML description spacing across all dbt YAML files.

This script ensures consistent spacing in YAML description blocks:
1. Two blank lines above headers (lines ending with :) 
2. One blank line above each bullet point
3. Removes excessive blank lines between consecutive bullet points

Usage:
    python fix_yaml_description_spacing.py [directory]
    
Arguments:
    directory: Root directory to process (default: models)
    
Examples:
    python fix_yaml_description_spacing.py                    # Process models/ directory
    python fix_yaml_description_spacing.py models/marts       # Process specific subdirectory
"""

import os
import sys
import argparse
from pathlib import Path

def fix_description_spacing(content):
    """
    Fix spacing in YAML description blocks.
    
    Returns:
        str: Content with fixed spacing
    """
    lines = content.split('\n')
    fixed_lines = []
    in_description = False
    description_start_index = None
    first_content_line_processed = False
    
    for i, line in enumerate(lines):
        # Check if we're starting a description block
        if 'description:' in line and not in_description:
            in_description = True
            description_start_index = len(fixed_lines)
            first_content_line_processed = False
            fixed_lines.append(line)
            continue
        
        # Check if we're ending a description block (new key at same or lesser indentation)
        if in_description and line.strip():
            # Get the indentation of the current line
            current_indent = len(line) - len(line.lstrip())
            # Get the indentation of the description line
            desc_line = fixed_lines[description_start_index] if description_start_index is not None else ""
            desc_indent = len(desc_line) - len(desc_line.lstrip())
            
            # If current line has same or less indentation and contains a colon (new key), we're out of description
            if current_indent <= desc_indent and ':' in line and not line.strip().startswith('•'):
                in_description = False
                description_start_index = None
        
        if in_description:
            stripped = line.strip()
            
            # Mark that we've processed the first content line
            if not first_content_line_processed and stripped:
                first_content_line_processed = True
            
            # Check if this is a bullet point
            is_bullet = (stripped.startswith('•') or 
                        stripped.startswith('-') or 
                        stripped.startswith('*'))
            
            # Check if this line is a header (ends with :) and is not a bullet point
            is_header = (stripped.endswith(':') and 
                        not is_bullet and
                        len(stripped) > 1)
            
            # Handle spacing logic
            if is_header:
                # Add two blank lines above ALL headers (not the very first line of description)
                if (fixed_lines and fixed_lines[-1].strip() and first_content_line_processed):
                    # Remove any existing blank lines first
                    while fixed_lines and not fixed_lines[-1].strip():
                        fixed_lines.pop()
                    # Add exactly two blank lines
                    fixed_lines.extend(['', ''])
            elif is_bullet:
                # Add one blank line above ALL bullet points
                # Remove any existing blank lines first
                while fixed_lines and not fixed_lines[-1].strip():
                    fixed_lines.pop()
                # Add exactly one blank line
                fixed_lines.append('')
            
            fixed_lines.append(line)
        else:
            fixed_lines.append(line)
    
    return '\n'.join(fixed_lines)

def fix_specific_headers(content, headers=None):
    """
    Fix spacing above specific headers (like "Business Logic:").
    
    Args:
        content (str): File content
        headers (list): List of header patterns to fix (default: ["Business Logic:"])
        
    Returns:
        str: Content with fixed header spacing
    """
    if headers is None:
        headers = ["Business Logic:"]
    
    lines = content.split('\n')
    fixed_lines = []
    
    for i, line in enumerate(lines):
        # Check if this line contains any of the target headers
        for header in headers:
            if line.strip().endswith(header):
                # Remove existing blank lines above
                while fixed_lines and not fixed_lines[-1].strip():
                    fixed_lines.pop()
                
                # Add exactly 2 blank lines
                fixed_lines.extend(['', ''])
                break
        
        fixed_lines.append(line)
    
    return '\n'.join(fixed_lines)

def process_yaml_files(directory, fix_headers=True, headers=None):
    """
    Process all YAML files in directory and subdirectories.
    
    Args:
        directory (str): Directory path to process
        fix_headers (bool): Whether to apply specific header fixes
        headers (list): Specific headers to fix spacing for
        
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
            
            # Apply general description spacing fixes
            fixed_content = fix_description_spacing(original_content)
            
            # Apply specific header fixes if requested
            if fix_headers:
                fixed_content = fix_specific_headers(fixed_content, headers)
            
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
  %(prog)s models --no-headers          # Skip specific header fixes
  %(prog)s models --headers "Key Features:" "Population Scope:"  # Fix specific headers
        """
    )
    
    parser.add_argument(
        'directory',
        nargs='?',
        default='models',
        help='Directory to process (default: models)'
    )
    
    parser.add_argument(
        '--no-headers',
        action='store_true',
        help='Skip specific header spacing fixes'
    )
    
    parser.add_argument(
        '--headers',
        nargs='*',
        default=["Business Logic:", "Population Scope:", "Key Features:"],
        help='Specific headers to fix spacing for'
    )
    
    args = parser.parse_args()
    
    # Validate directory exists
    if not os.path.exists(args.directory):
        print(f"Error: Directory '{args.directory}' not found")
        sys.exit(1)
    
    # Process files
    fix_headers = not args.no_headers
    total_files, processed_files = process_yaml_files(
        args.directory, 
        fix_headers=fix_headers,
        headers=args.headers if fix_headers else None
    )
    
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