#!/usr/bin/env python3
"""
Run the complete sources workflow: extract metadata -> generate sources -> generate staging models -> generate schema.
This script orchestrates the entire process from Snowflake metadata extraction to complete staging model setup.
"""

import subprocess
import sys
import os
import argparse
from pathlib import Path

# Color codes for better output
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    WHITE = '\033[97m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'

def print_step(step_num, total_steps, description):
    """Print a colored step header."""
    print(f"\n{Colors.BOLD}{Colors.CYAN}[{step_num}/{total_steps}] {description}{Colors.END}")
    print(f"{Colors.BLUE}{'='*60}{Colors.END}")

def print_success(message):
    """Print a success message in green."""
    print(f"{Colors.GREEN}✓ {message}{Colors.END}")

def print_error(message):
    """Print an error message in red."""
    print(f"{Colors.RED}✗ {message}{Colors.END}")

def print_warning(message):
    """Print a warning message in yellow."""
    print(f"{Colors.YELLOW}⚠ {message}{Colors.END}")

def print_info(message):
    """Print an info message in white."""
    print(f"{Colors.WHITE}ℹ {message}{Colors.END}")

# Path configuration
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = Path(CURRENT_DIR).parent.parent

def run_script(script_name, args=None, description="", step_num=1, total_steps=5):
    """Run a Python script and handle errors."""
    print_step(step_num, total_steps, description)
    print_info(f"Running: {script_name}")
    
    cmd = [sys.executable, script_name]
    if args:
        cmd.extend(args)
    
    try:
        result = subprocess.run(
            cmd, 
            cwd=CURRENT_DIR,
            check=True,
            capture_output=True,  # Capture output for cleaner display
            text=True
        )
        
        # Show only important output lines
        if result.stdout:
            lines = result.stdout.strip().split('\n')
            # Show key progress lines
            for line in lines:
                if any(keyword in line.lower() for keyword in ['created', 'generated', 'success', 'complete', 'models', 'tables']):
                    print_info(f"  {line}")
        
        print_success(f"{script_name} completed successfully")
        return True
        
    except subprocess.CalledProcessError as e:
        print_error(f"{script_name} failed with exit code {e.returncode}")
        if e.stderr:
            print_error(f"Error: {e.stderr}")
        return False
    except Exception as e:
        print_error(f"Unexpected error running {script_name}: {e}")
        return False

def load_preserved_models():
    """Load list of models to preserve from the ignore list file."""
    preserved_models_file = Path(CURRENT_DIR) / 'sources_ignore_list.yml'
    preserved = set()
    
    if preserved_models_file.exists():
        import yaml
        with open(preserved_models_file, 'r') as f:
            data = yaml.safe_load(f)
            
        if data and 'preserved_models' in data:
            for item in data['preserved_models']:
                preserved.add(item['model'])
    
    return preserved

def cleanup_legacy_files():
    """Remove legacy sources.yml, schema.yml, and base layer files before regeneration."""
    # Note: We only clean up schema, sources, and base layer files, not staging models
    # Staging models are handled by the generation script which respects preserved models
    legacy_files = [
        PROJECT_DIR / 'models' / 'sources.yml',
        PROJECT_DIR / 'models' / 'staging' / 'schema.yml',
        PROJECT_DIR / 'models' / 'olids' / 'staging' / 'schema.yml',
        PROJECT_DIR / 'models' / 'shared' / 'staging' / 'schema.yml',
        PROJECT_DIR / 'models' / 'olids' / 'base' / 'schema.yml',
        PROJECT_DIR / 'sources.yml',
        PROJECT_DIR / 'schema.yml'
    ]
    
    removed_count = 0
    for file_path in legacy_files:
        if file_path.exists():
            print_info(f"  Removing legacy file: {file_path.relative_to(PROJECT_DIR)}")
            file_path.unlink()
            removed_count += 1
    
    # Clean up base layer SQL files (except base_olids_patient_filtered.sql and int_ncl_practices.sql)
    base_layer_dir = PROJECT_DIR / 'models' / 'olids' / 'base'
    if base_layer_dir.exists():
        preserved_base_models = {'base_olids_patient_filtered.sql'}  # Keep manually created helper
        for sql_file in base_layer_dir.glob('base_olids_*.sql'):
            if sql_file.name not in preserved_base_models:
                print_info(f"  Removing generated base model: {sql_file.relative_to(PROJECT_DIR)}")
                sql_file.unlink()
                removed_count += 1
    
    return removed_count

def main():
    """Run the complete sources workflow."""
    parser = argparse.ArgumentParser(description='Run complete sources workflow')
    parser.add_argument('--skip-extract', action='store_true',
                       help='Skip metadata extraction (use existing table_metadata.csv)')
    parser.add_argument('--no-schema', action='store_true',
                       help='Skip schema.yml generation (default: generate schema files with tests)')
    parser.add_argument('--keep-legacy', action='store_true',
                       help='Keep existing sources.yml and schema.yml files (default: clean start)')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be done without executing')
    
    args = parser.parse_args()
    
    print(f"\n{Colors.BOLD}{Colors.MAGENTA}OLIDS Sources Workflow{Colors.END}")
    print(f"{Colors.MAGENTA}{'='*60}{Colors.END}")
    print_info("This workflow will:")
    if not args.skip_extract:
        print_info("  1. Generate metadata query from source mappings")
        print_info("  2. Extract metadata from Snowflake")
    else:
        print_warning("  1-2. SKIPPED: Using existing metadata")
    print_info("  3. Generate sources.yml from metadata")
    print_info("  4. Generate base layer views (filtered OLIDS data)")
    print_info("  5. Generate base layer schema.yml")
    print_info("  6. Generate staging models from sources")
    if not args.no_schema:
        print_info("  7. Generate staging schema.yml files with tests")
    else:
        print_warning("  7. SKIPPED: Schema generation disabled")
    print()
    
    if args.dry_run:
        print_warning("DRY RUN MODE - No changes will be made")
        return
    
    # Calculate total steps first
    success_count = 0
    # Core steps: Generate Sources + Generate Base Layer + Generate Base Schema + Generate Models (with auto-schema by default)
    total_steps = 4  
    if not args.skip_extract:
        total_steps += 2  # Add: Generate Query + Extract Metadata
    if args.no_schema:
        total_steps += 1  # Add: Separate Schema Generation step
    
    # Clean up legacy files by default (unless --keep-legacy specified)
    if not args.keep_legacy:
        print_step(0, total_steps, "Clean up legacy sources and schema files")
        removed_count = cleanup_legacy_files()
        if removed_count > 0:
            print_success(f"Removed {removed_count} legacy files")
        else:
            print_info("No legacy files found to remove")
    
    step_num = 1
    
    # Step 1: Generate metadata query (if not skipping)
    if not args.skip_extract:
        if run_script("1a_generate_metadata_query.py", 
                     description="Generate dynamic metadata query from source mappings",
                     step_num=step_num, total_steps=total_steps):
            success_count += 1
            step_num += 1
        else:
            print_error("Workflow stopped due to error in step 1")
            sys.exit(1)
        
        # Step 2: Extract metadata
        if run_script("1b_extract_metadata.py", 
                     description="Extract metadata from Snowflake using Snowpark",
                     step_num=step_num, total_steps=total_steps):
            success_count += 1
            step_num += 1
        else:
            print_error("Workflow stopped due to error in step 2")
            print_warning("Note: Make sure you have Snowflake credentials configured")
            sys.exit(1)
    else:
        print_warning("SKIPPING: Metadata extraction (using existing table_metadata.csv)")
        success_count += 2
        step_num = 3
    
    # Step 3: Generate sources.yml
    if run_script("2_generate_sources.py", 
                 description="Generate sources.yml from extracted metadata",
                 step_num=step_num, total_steps=total_steps):
        success_count += 1
        step_num += 1
    else:
        print_error("Workflow stopped due to error in step 3")
        sys.exit(1)
    
    # Step 4: Generate base layer views
    if run_script("3_generate_base_layer.py", 
                 description="Generate filtered base layer views for OLIDS data",
                 step_num=step_num, total_steps=total_steps):
        success_count += 1
        step_num += 1
    else:
        print_error("Workflow stopped due to error in step 4")
        sys.exit(1)
    
    # Step 5: Generate base layer schema
    if run_script("3b_generate_base_schema.py", 
                 description="Generate schema.yml for base layer models",
                 step_num=step_num, total_steps=total_steps):
        success_count += 1
        step_num += 1
    else:
        print_error("Workflow stopped due to error in step 5")
        sys.exit(1)
    
    # Step 6: Generate staging models
    staging_args = []
    if not args.no_schema:
        staging_args.append("--auto-schema")
    
    if run_script("4_generate_staging_models.py", 
                 args=staging_args,
                 description="Generate staging models from sources.yml",
                 step_num=step_num, total_steps=total_steps):
        success_count += 1
        step_num += 1
    else:
        print_error("Workflow stopped due to error in step 6")
        sys.exit(1)
    
    # Step 7: Generate schema files (if not done automatically in step 6)
    if args.no_schema:
        if run_script("5_generate_staging_schema.py", 
                     description="Generate schema.yml files with default tests",
                     step_num=step_num, total_steps=total_steps):
            success_count += 1
        else:
            print_error("Workflow stopped due to error in step 7")
            sys.exit(1)
    
    # Summary
    print(f"\n{Colors.BOLD}{Colors.MAGENTA}{'='*60}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}WORKFLOW COMPLETE{Colors.END}")
    print(f"{Colors.MAGENTA}{'='*60}{Colors.END}")
    print_info(f"Successfully completed {success_count}/{total_steps} steps")
    
    if success_count == total_steps:
        print_success("All steps completed successfully!")
        print(f"\n{Colors.BOLD}Next steps:{Colors.END}")
        print_info("  1. Review generated files:")
        print_info("     - models/sources.yml")
        print_info("     - models/*/staging/*.sql")
        print_info("     - models/*/staging/schema.yml")
        print_info("  2. Test the setup:")
        print_info("     dbt run --select staging")
        print_info("     dbt test --select staging")
    else:
        print_error("Some steps failed. Check the output above for details.")
        sys.exit(1)

if __name__ == '__main__':
    main()