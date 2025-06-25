#!/usr/bin/env python3
"""
Check for YAML column-level tests that reference non-existent columns.

This script:
1. Reads compiled SQL files from target/run directory
2. Extracts column names from SELECT statements
3. Compares with column-level tests in YAML files
4. Reports tests for columns that don't exist in the model

Usage:
    python scripts/check_yaml_column_tests.py [--fix-orphans]

    --fix-orphans    Remove tests for columns that don't exist in the model
"""

import argparse
import re
import sys
import subprocess
import json
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
import yaml
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def get_all_model_columns_via_dbt() -> Dict[str, Set[str]]:
    """
    Get column names for all models using dbt macro and utility model.

    Returns:
        Dict mapping model names to sets of column names (lowercase)
    """
    model_columns = {}

    try:
        # Run the utility model to get column information
        logger.info("Running dbt utility model to extract column information...")
        cmd = ['dbt', 'run', '--select', 'utility_get_model_columns']
        result = subprocess.run(cmd, capture_output=True, text=True, cwd='.')

        if result.returncode != 0:
            logger.error(f"Failed to run utility model: {result.stderr}")
            return model_columns

        # Now query the result to get the JSON
        cmd = ['dbt', 'show', '--select', 'utility_get_model_columns', '--limit', '1', '--output', 'json']
        result = subprocess.run(cmd, capture_output=True, text=True, cwd='.')

        if result.returncode != 0:
            logger.error(f"Failed to query utility model: {result.stderr}")
            return model_columns

        # Extract JSON from dbt output (handle the fact that it might all be on one line)
        stdout = result.stdout.strip()

        # Find the start of JSON (look for the first '{' after logging)
        json_start = stdout.find('{\n  "node"')
        if json_start == -1:
            # Try alternative pattern
            json_start = stdout.find('{"node"')

        if json_start == -1:
            logger.error("Could not find JSON in dbt output")
            return model_columns

        json_output = stdout[json_start:]
        output_data = json.loads(json_output)

        # Extract the JSON string from the dbt output
        if 'show' in output_data and output_data['show']:
            row_data = output_data['show'][0]

            # Handle the case where row_data is a dict (which it is)
            if isinstance(row_data, dict) and 'MODEL_COLUMNS_JSON' in row_data:
                json_str = row_data['MODEL_COLUMNS_JSON']
                columns_data = json.loads(json_str)

                # Convert to the expected format
                for model_name, column_list in columns_data.items():
                    model_columns[model_name] = set(col.lower() for col in column_list)
            elif isinstance(row_data, list) and len(row_data) > 0:
                # Fallback for list format
                json_str = row_data[0]  # First column is our JSON
                columns_data = json.loads(json_str)

                # Convert to the expected format
                for model_name, column_list in columns_data.items():
                    model_columns[model_name] = set(col.lower() for col in column_list)

        logger.info(f"Successfully extracted column information for {len(model_columns)} models")

    except Exception as e:
        logger.error(f"Error getting model columns via dbt: {e}")

    return model_columns


def get_model_columns_for_single_model(model_name: str) -> Set[str]:
    """
    Get column names for a single model using dbt show.
    Fallback method when the bulk approach fails.

    Args:
        model_name: The dbt model name

    Returns:
        Set of column names (lowercase)
    """
    columns = set()

    try:
        # Use dbt show with limit 0 to get just schema
        cmd = ['dbt', 'show', '--select', model_name, '--limit', '0', '--output', 'json']
        result = subprocess.run(cmd, capture_output=True, text=True, cwd='.')

        if result.returncode != 0:
            logger.warning(f"dbt show failed for {model_name}: {result.stderr}")
            return columns

        # The JSON output should contain column information
        output_data = json.loads(result.stdout)

        # For limit 0, we still get column headers
        if 'show' in output_data:
            # Extract column names from the structure
            # This is a simpler fallback - we could enhance this if needed
            logger.debug(f"Got dbt show output for {model_name}")

    except Exception as e:
        logger.warning(f"Error getting columns for {model_name}: {e}")

    return columns


def get_yaml_column_tests(yaml_path: Path) -> Dict[str, List[str]]:
    """
    Extract column names that have tests defined in YAML file.

    Args:
        yaml_path: Path to the YAML file

    Returns:
        Dict mapping column names to list of test names
    """
    column_tests = {}

    try:
        with open(yaml_path, 'r', encoding='utf-8') as f:
            yaml_content = yaml.safe_load(f)

        if not yaml_content or 'models' not in yaml_content:
            return column_tests

        for model in yaml_content['models']:
            if 'columns' in model:
                for column in model['columns']:
                    column_name = column.get('name', '').lower()
                    if column_name and 'tests' in column:
                        tests = column['tests']
                        test_names = []

                        for test in tests:
                            if isinstance(test, str):
                                test_names.append(test)
                            elif isinstance(test, dict):
                                test_names.extend(test.keys())

                        if test_names:
                            column_tests[column_name] = test_names

    except Exception as e:
        logger.warning(f"Error reading YAML file {yaml_path}: {e}")

    return column_tests


def find_models_to_check() -> List[Tuple[str, Path]]:
    """
    Find models with YAML files that can be checked.

    Returns:
        List of (model_name, yaml_path) tuples
    """
    project_root = Path(__file__).parent.parent
    models_dir = project_root / "models"

    model_pairs = []

    # Find all YAML files in models directory
    for yaml_path in models_dir.rglob("*.yml"):
        # Skip if it's a directory or temporary file
        if yaml_path.is_dir() or yaml_path.name.startswith('.'):
            continue

        # Extract model name from YAML filename
        model_name = yaml_path.stem

        # Skip files that don't look like model YAML files
        if model_name in ['sources', 'schema']:
            continue

        model_pairs.append((model_name, yaml_path))

    return model_pairs


def remove_orphaned_tests(yaml_path: Path, orphaned_columns: Set[str]) -> bool:
    """
    Remove tests for columns that don't exist in the model.

    Args:
        yaml_path: Path to YAML file
        orphaned_columns: Set of column names to remove tests for

    Returns:
        True if file was modified, False otherwise
    """
    try:
        with open(yaml_path, 'r', encoding='utf-8') as f:
            yaml_content = yaml.safe_load(f)

        if not yaml_content or 'models' not in yaml_content:
            return False

        modified = False

        for model in yaml_content['models']:
            if 'columns' in model:
                # Filter out columns that are orphaned
                original_columns = model['columns']
                filtered_columns = []

                for column in original_columns:
                    column_name = column.get('name', '').lower()
                    if column_name not in orphaned_columns:
                        filtered_columns.append(column)
                    else:
                        logger.info(f"Removing tests for orphaned column '{column_name}' from {yaml_path.name}")
                        modified = True

                model['columns'] = filtered_columns

        if modified:
            with open(yaml_path, 'w', encoding='utf-8') as f:
                yaml.dump(yaml_content, f, default_flow_style=False, sort_keys=False, width=120)

        return modified

    except Exception as e:
        logger.error(f"Error modifying YAML file {yaml_path}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description='Check for YAML column tests that reference non-existent columns')
    parser.add_argument('--fix-orphans', action='store_true',
                       help='Remove tests for columns that don\'t exist in the model')
    parser.add_argument('--issues-only', action='store_true',
                       help='Only show models with issues')

    args = parser.parse_args()

    # Get project root
    project_root = Path(__file__).parent.parent

    logger.info("Finding models with YAML files...")
    model_pairs = find_models_to_check()

    if not model_pairs:
        logger.error("No model YAML files found.")
        sys.exit(1)

    logger.info(f"Found {len(model_pairs)} models to check")

    # Get column information for all models at once
    logger.info("Extracting column information for all models using dbt...")
    all_model_columns = get_all_model_columns_via_dbt()

    if not all_model_columns:
        logger.error("Could not extract model column information. Ensure models are built and accessible.")
        sys.exit(1)

    total_issues = 0
    total_orphaned_tests = 0
    total_fixed_files = 0

    for model_name, yaml_path in model_pairs:
        # Get columns for this model
        sql_columns = all_model_columns.get(model_name, set())

        if not sql_columns:
            logger.warning(f"No column information found for model {model_name} - skipping")
            continue

        # Extract column tests from YAML
        yaml_column_tests = get_yaml_column_tests(yaml_path)

        # Find orphaned tests (tests for columns that don't exist)
        orphaned_columns = set(yaml_column_tests.keys()) - sql_columns

        if orphaned_columns:
            total_issues += 1
            orphaned_count = len(orphaned_columns)
            total_orphaned_tests += orphaned_count

            if not args.issues_only:
                logger.info(f"\n📄 {model_name}")
                logger.info(f"   SQL columns ({len(sql_columns)}): {sorted(sql_columns)}")
                logger.info(f"   YAML test columns ({len(yaml_column_tests)}): {sorted(yaml_column_tests.keys())}")

            logger.warning(f"🚨 {model_name}: {orphaned_count} orphaned column tests")
            for col in sorted(orphaned_columns):
                tests = yaml_column_tests[col]
                logger.warning(f"   - Column '{col}' has tests {tests} but doesn't exist in model")

            # Fix orphaned tests if requested
            if args.fix_orphans:
                if remove_orphaned_tests(yaml_path, orphaned_columns):
                    total_fixed_files += 1
                    logger.info(f"✅ Fixed {yaml_path.name}")

        elif not args.issues_only:
            logger.info(f"✅ {model_name}: All column tests reference valid columns")

    # Summary
    logger.info(f"\n📊 SUMMARY")
    logger.info(f"Models checked: {len(model_pairs)}")
    logger.info(f"Models with issues: {total_issues}")
    logger.info(f"Total orphaned column tests: {total_orphaned_tests}")

    if args.fix_orphans:
        logger.info(f"YAML files fixed: {total_fixed_files}")

    if total_issues > 0:
        logger.warning(f"Found {total_issues} models with orphaned column tests")
        if not args.fix_orphans:
            logger.info("Run with --fix-orphans to automatically remove these tests")
        sys.exit(1)
    else:
        logger.info("✅ All column tests reference valid columns!")


if __name__ == "__main__":
    main()
