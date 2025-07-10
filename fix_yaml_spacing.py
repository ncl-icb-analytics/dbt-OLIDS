#!/usr/bin/env python3
"""
Fix YAML description spacing by adding blank lines above headers (lines ending with :)
Ignores the first line and lines that start with bullets (•)
"""

import os
import re
from pathlib import Path

def fix_description_spacing(content):
    """Fix spacing in YAML description blocks"""
    lines = content.split('\n')
    fixed_lines = []
    in_description = False
    description_start_index = None
    
    for i, line in enumerate(lines):
        # Check if we're starting a description block
        if 'description:' in line and not in_description:
            in_description = True
            description_start_index = len(fixed_lines)
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
            # Check if this line is a header (ends with :) and is not a bullet point
            stripped = line.strip()
            if (stripped.endswith(':') and 
                not stripped.startswith('•') and 
                not stripped.startswith('-') and
                not stripped.startswith('*') and
                len(stripped) > 1):
                
                # Add blank line above any header (except if previous line is already empty)
                # Only skip if this would be the very first content (opening quote line)
                has_description_content = any(fixed_lines[description_start_index+1:])
                
                # Check if previous line is not empty 
                if (fixed_lines and fixed_lines[-1].strip() and has_description_content):
                    # Add a blank line before the header
                    fixed_lines.append('')
            
            fixed_lines.append(line)
        else:
            fixed_lines.append(line)
    
    return '\n'.join(fixed_lines)

def process_yaml_files(directory):
    """Process all YAML files in directory and subdirectories"""
    yaml_files = []
    
    # Find all .yml files
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.yml'):
                yaml_files.append(os.path.join(root, file))
    
    print(f"Found {len(yaml_files)} YAML files to process")
    
    processed = 0
    for yaml_file in yaml_files:
        try:
            with open(yaml_file, 'r', encoding='utf-8') as f:
                original_content = f.read()
            
            fixed_content = fix_description_spacing(original_content)
            
            # Only write if content changed
            if fixed_content != original_content:
                with open(yaml_file, 'w', encoding='utf-8') as f:
                    f.write(fixed_content)
                processed += 1
                print(f"Fixed spacing in: {yaml_file}")
        
        except Exception as e:
            print(f"Error processing {yaml_file}: {e}")
    
    print(f"Successfully processed {processed} files")

if __name__ == "__main__":
    # Process models directory
    models_dir = "models"
    if os.path.exists(models_dir):
        process_yaml_files(models_dir)
    else:
        print(f"Directory {models_dir} not found")