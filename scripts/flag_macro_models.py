#!/usr/bin/env python3
"""
Flag models that use get_observations or get_medication_orders macros.

This script identifies all dbt models that use the get_observations() or
get_medication_orders() macros and flags their corresponding YAML files
for appropriate testing (cluster_ids_exist or bnf_codes_exist tests).

Usage:
    python scripts/flag_macro_models.py
"""

import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
import yaml


def extract_cluster_ids_from_get_observations(content: str) -> Set[str]:
    """Extract cluster IDs from get_observations macro calls."""
    cluster_ids = set()

    # Pattern to match get_observations("content") and extract what's inside the quotes
    # Looking for double quotes containing content (which may include single quotes)
    pattern = r'get_observations\(\s*"([^"]+)"\s*\)'
    matches = re.findall(pattern, content, re.IGNORECASE)

    for match in matches:
        # The match contains the full string like "'CHD_COD', 'OTHER_COD'"
        # We need to extract individual cluster IDs that are wrapped in single quotes
        individual_ids = re.findall(r"'([^']+)'", match)
        cluster_ids.update(individual_ids)

    return cluster_ids


def extract_bnf_codes_from_get_medication_orders(content: str) -> Set[str]:
    """Extract BNF codes from get_medication_orders macro calls."""
    bnf_codes = set()

    # Pattern for bnf_code parameter: get_medication_orders(bnf_code='123456')
    bnf_pattern = r'get_medication_orders\([^)]*bnf_code\s*=\s*["\']([^"\']+)["\']'
    matches = re.findall(bnf_pattern, content, re.IGNORECASE)

    bnf_codes.update(matches)

    return bnf_codes


def extract_cluster_ids_from_medication_orders(content: str) -> Set[str]:
    """Extract cluster IDs from get_medication_orders macro calls."""
    cluster_ids = set()

    # Pattern for cluster_id parameter: get_medication_orders(cluster_id='ID')
    cluster_pattern = r'get_medication_orders\([^)]*cluster_id\s*=\s*["\']([^"\']+)["\']'
    matches = re.findall(cluster_pattern, content, re.IGNORECASE)

    cluster_ids.update(matches)

    return cluster_ids


def check_yaml_for_tests(yaml_path: Path, sql_bnf_codes: Set[str] = None) -> Dict[str, bool]:
    """Check if YAML file contains the appropriate tests."""
    tests_present = {
        'cluster_ids_exist': False,
        'bnf_codes_exist': False
    }

    if not yaml_path.exists():
        return tests_present

    try:
        with open(yaml_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Look for model-level tests (not in columns section)
        lines = content.split('\n')
        model_name = yaml_path.stem
        in_model = False
        in_model_tests = False

        for i, line in enumerate(lines):
            # Find the specific model
            if f"- name: {model_name}" in line:
                in_model = True
                continue

            if in_model:
                # Check if we're in the model-level tests section (indent level 4)
                if line.strip() == 'tests:' and len(line) - len(line.lstrip()) == 4:
                    in_model_tests = True
                    continue

                # If we hit another model or section at the same level, we're done
                elif (line.strip().startswith('- name:') or
                      (line.strip() and len(line) - len(line.lstrip()) == 4 and
                       not line.strip().startswith('tests:'))):
                    break

                # Check for tests in the model tests section
                if in_model_tests and line.strip().startswith('- test_'):
                    if 'test_cluster_ids_exist' in line:
                        tests_present['cluster_ids_exist'] = True
                    elif 'test_bnf_codes_exist' in line:
                        # For BNF codes, check if there's a prefix match
                        if sql_bnf_codes:
                            # Extract BNF codes from this line or subsequent lines
                            import re
                            bnf_match = re.search(r'bnf_codes:\s*["\']([^"\']+)["\']', line)

                            # If not found on same line, check next few lines
                            if not bnf_match:
                                for j in range(i + 1, min(i + 5, len(lines))):  # Check next 4 lines
                                    next_line = lines[j]
                                    bnf_match = re.search(r'bnf_codes:\s*["\']([^"\']+)["\']', next_line)
                                    if bnf_match:
                                        break

                            if bnf_match:
                                yaml_bnf_codes = [code.strip() for code in bnf_match.group(1).split(',')]
                                # Check if any YAML BNF code is a prefix of any SQL BNF code
                                for sql_code in sql_bnf_codes:
                                    for yaml_code in yaml_bnf_codes:
                                        if sql_code.startswith(yaml_code):
                                            tests_present['bnf_codes_exist'] = True
                                            break
                                    if tests_present['bnf_codes_exist']:
                                        break
                        else:
                            # If no SQL codes provided, just check for presence
                            tests_present['bnf_codes_exist'] = True

    except Exception as e:
        print(f"Warning: Could not read {yaml_path}: {e}")

    return tests_present


def find_models_using_macros(models_dir: Path) -> List[Dict]:
    """Find all models using get_observations or get_medication_orders macros."""
    results = []

    for sql_file in models_dir.rglob('*.sql'):
        try:
            with open(sql_file, 'r', encoding='utf-8') as f:
                content = f.read()

            # Check for macro usage
            uses_get_observations = 'get_observations(' in content
            uses_get_medication_orders = 'get_medication_orders(' in content

            if not (uses_get_observations or uses_get_medication_orders):
                continue

            # Extract parameters
            cluster_ids_from_obs = set()
            bnf_codes = set()
            cluster_ids_from_meds = set()

            if uses_get_observations:
                cluster_ids_from_obs = extract_cluster_ids_from_get_observations(content)

            if uses_get_medication_orders:
                bnf_codes = extract_bnf_codes_from_get_medication_orders(content)
                cluster_ids_from_meds = extract_cluster_ids_from_medication_orders(content)

            # All cluster IDs from both sources
            all_cluster_ids = cluster_ids_from_obs | cluster_ids_from_meds

            # Find corresponding YAML file
            yaml_file = sql_file.with_suffix('.yml')
            tests_present = check_yaml_for_tests(yaml_file, bnf_codes)

            # Determine what tests should be present
            needs_cluster_test = len(all_cluster_ids) > 0
            needs_bnf_test = len(bnf_codes) > 0

            model_info = {
                'sql_file': sql_file,
                'yaml_file': yaml_file,
                'yaml_exists': yaml_file.exists(),
                'uses_get_observations': uses_get_observations,
                'uses_get_medication_orders': uses_get_medication_orders,
                'cluster_ids': sorted(all_cluster_ids),
                'bnf_codes': sorted(bnf_codes),
                'needs_cluster_test': needs_cluster_test,
                'needs_bnf_test': needs_bnf_test,
                'has_cluster_test': tests_present['cluster_ids_exist'],
                'has_bnf_test': tests_present['bnf_codes_exist']
            }

            results.append(model_info)

        except Exception as e:
            print(f"Error processing {sql_file}: {e}")

    return results


def categorise_models(models: List[Dict]) -> Dict[str, List[Dict]]:
    """Categorise models by their test status."""
    categories = {
        'missing_yaml': [],
        'missing_cluster_test': [],
        'missing_bnf_test': [],
        'all_tests_present': [],
        'has_extra_tests': []
    }

    for model in models:
        if not model['yaml_exists']:
            categories['missing_yaml'].append(model)
        elif model['needs_cluster_test'] and not model['has_cluster_test']:
            categories['missing_cluster_test'].append(model)
        elif model['needs_bnf_test'] and not model['has_bnf_test']:
            categories['missing_bnf_test'].append(model)
        elif (model['needs_cluster_test'] == model['has_cluster_test'] and
              model['needs_bnf_test'] == model['has_bnf_test']):
            categories['all_tests_present'].append(model)
        else:
            categories['has_extra_tests'].append(model)

    return categories


def print_summary(categories: Dict[str, List[Dict]]) -> None:
    """Print a summary of findings."""
    total_models = sum(len(models) for models in categories.values())

    print(f"\n{'='*80}")
    print(f"MACRO USAGE ANALYSIS SUMMARY")
    print(f"{'='*80}")
    print(f"Total models using get_observations/get_medication_orders: {total_models}")
    print()

    for category, models in categories.items():
        if not models:
            continue

        print(f"{category.replace('_', ' ').title()}: {len(models)} models")

        for model in models:
            rel_path = str(model['sql_file']).replace(str(Path.cwd()), '.')
            print(f"  • {rel_path}")

            if model['uses_get_observations']:
                cluster_ids_str = ', '.join(sorted(model['cluster_ids']))
                print(f"    - Uses get_observations with cluster IDs: {cluster_ids_str}")

            if model['uses_get_medication_orders']:
                if model['bnf_codes']:
                    bnf_codes_str = ', '.join(sorted(model['bnf_codes']))
                    print(f"    - Uses get_medication_orders with BNF codes: {bnf_codes_str}")
                if model['cluster_ids']:
                    cluster_ids_str = ', '.join(sorted(model['cluster_ids']))
                    print(f"    - Uses get_medication_orders with cluster IDs: {cluster_ids_str}")

            if not model['yaml_exists']:
                print(f"    ⚠️  Missing YAML file: {model['yaml_file'].name}")
            else:
                missing_tests = []
                if model['needs_cluster_test'] and not model['has_cluster_test']:
                    missing_tests.append('test_cluster_ids_exist')
                if model['needs_bnf_test'] and not model['has_bnf_test']:
                    missing_tests.append('test_bnf_codes_exist')

                if missing_tests:
                    print(f"    ⚠️  Missing tests: {', '.join(missing_tests)}")
                elif category == 'all_tests_present':
                    print(f"    ✅ All required tests present")

        print()


def add_missing_tests_to_yaml(models_needing_tests: List[Dict], dry_run: bool = True) -> None:
    """Add missing tests to YAML files."""

    files_updated = 0

    for model in models_needing_tests:
        yaml_path = model['yaml_file']

        if not yaml_path.exists():
            print(f"⚠️  Skipping {yaml_path.name} - file doesn't exist")
            continue

        try:
            # Read the existing YAML as text first to preserve formatting
            with open(yaml_path, 'r', encoding='utf-8') as f:
                yaml_text = f.read()

            # Also load as dict for manipulation
            with open(yaml_path, 'r', encoding='utf-8') as f:
                yaml_content = yaml.safe_load(f)

            # Find the model in the YAML
            model_name = yaml_path.stem  # e.g., 'int_bmi_all' from 'int_bmi_all.yml'
            model_config = None

            if 'models' in yaml_content:
                for model_def in yaml_content['models']:
                    if model_def.get('name') == model_name:
                        model_config = model_def
                        break

            if not model_config:
                print(f"⚠️  Skipping {yaml_path.name} - couldn't find model definition")
                continue

            tests_to_add = []

            # Check what tests need to be added
            if model['needs_cluster_test'] and not model['has_cluster_test']:
                cluster_ids_str = ", ".join(sorted(model['cluster_ids']))
                tests_to_add.append(f"test_cluster_ids_exist (cluster_ids: {cluster_ids_str})")

            if model['needs_bnf_test'] and not model['has_bnf_test']:
                bnf_codes_str = ", ".join(sorted(model['bnf_codes']))
                tests_to_add.append(f"test_bnf_codes_exist (bnf_codes: {bnf_codes_str})")

            if tests_to_add:
                if dry_run:
                    print(f"📋 Would update {yaml_path.name} with {len(tests_to_add)} test(s)")
                    for test in tests_to_add:
                        print(f"  + {test}")
                else:
                    # Add tests to YAML text manually to preserve formatting
                    new_yaml_text = add_tests_to_yaml_text(yaml_text, model, dry_run=False)

                    if new_yaml_text != yaml_text:
                        with open(yaml_path, 'w', encoding='utf-8') as f:
                            f.write(new_yaml_text)

                        print(f"✅ Updated {yaml_path.name} with {len(tests_to_add)} test(s)")
                        files_updated += 1
                    else:
                        print(f"⚠️  No changes made to {yaml_path.name}")

        except Exception as e:
            print(f"❌ Error processing {yaml_path.name}: {e}")

    if dry_run:
        print(f"\n📋 DRY RUN: Would update {len([m for m in models_needing_tests if m['yaml_file'].exists()])} YAML files")
        print("Run with --apply-tests to actually make the changes")
    else:
        print(f"\n✅ Successfully updated {files_updated} YAML files")


def add_tests_to_yaml_text(yaml_text: str, model: Dict, dry_run: bool = True) -> str:
    """Add tests to YAML text while preserving formatting."""
    lines = yaml_text.split('\n')

    # Find the model section and its tests section
    in_model = False
    model_name = model['yaml_file'].stem
    tests_section_indent = None
    insert_position = None
    columns_section_end = None

    for i, line in enumerate(lines):
        # Look for the model definition
        if f"- name: {model_name}" in line:
            in_model = True
            continue

        if in_model:
            # Look for existing model-level tests section (at same indent as columns:)
            if line.strip().startswith('tests:') and len(line) - len(line.lstrip()) == 4:
                tests_section_indent = len(line) - len(line.lstrip())
                # Find the end of the tests section to insert new tests
                for j in range(i + 1, len(lines)):
                    if lines[j].strip() and not lines[j].startswith(' ' * (tests_section_indent + 2)):
                        insert_position = j
                        break
                if insert_position is None:
                    insert_position = len(lines)
                break

            # Track the end of the columns section
            elif line.strip().startswith('columns:'):
                # Find the end of the columns section
                for j in range(i + 1, len(lines)):
                    # Look for next top-level section or another model
                    if (lines[j].strip() and
                        not lines[j].startswith('      ') and  # Not part of columns
                        (lines[j].strip().startswith('- name:') or  # Another model
                         (len(lines[j]) - len(lines[j].lstrip()) <= 4 and lines[j].strip()))):  # Same level as columns
                        columns_section_end = j
                        break
                if columns_section_end is None:
                    columns_section_end = len(lines)

            # If we find another model, end here
            elif line.strip().startswith('- name:') and model_name not in line:
                if columns_section_end:
                    insert_position = columns_section_end
                else:
                    insert_position = i
                break

    # If no tests section found but we have columns_section_end, use that
    if insert_position is None and columns_section_end:
        insert_position = columns_section_end
    elif insert_position is None:
        # Add to end of file
        insert_position = len(lines)

    # Build tests to add
    new_lines = []

    # Add blank line before tests section for readability
    if insert_position < len(lines) and lines[insert_position - 1].strip():
        new_lines.append('')

    # Add tests section header if it doesn't exist
    if tests_section_indent is None:
        new_lines.append('    tests:')
        test_indent = 6
    else:
        test_indent = tests_section_indent + 2

    # Add cluster test if needed
    if model['needs_cluster_test'] and not model['has_cluster_test']:
        cluster_ids_str = ", ".join(sorted(model['cluster_ids']))
        new_lines.extend([
            ' ' * test_indent + '- test_cluster_ids_exist:',
            ' ' * (test_indent + 4) + f'cluster_ids: "{cluster_ids_str}"'
        ])

    # Add BNF test if needed
    if model['needs_bnf_test'] and not model['has_bnf_test']:
        bnf_codes_str = ", ".join(sorted(model['bnf_codes']))
        new_lines.extend([
            ' ' * test_indent + '- test_bnf_codes_exist:',
            ' ' * (test_indent + 4) + f'bnf_codes: "{bnf_codes_str}"'
        ])

    # Insert the new lines
    lines[insert_position:insert_position] = new_lines

    return '\n'.join(lines)


def generate_test_snippets(categories: Dict[str, List[Dict]]) -> None:
    """Generate test snippets for missing tests."""
    models_needing_tests = (categories['missing_cluster_test'] +
                           categories['missing_bnf_test'])

    if not models_needing_tests:
        print("✅ No missing tests found!")
        return

    print(f"\n{'='*80}")
    print("SUGGESTED TEST ADDITIONS")
    print(f"{'='*80}")

    for model in models_needing_tests:
        print(f"\nFor {model['yaml_file'].name}:")
        print("Add to the tests section of the model:")

        if model['needs_cluster_test'] and not model['has_cluster_test']:
            cluster_ids_str = ", ".join(sorted(model['cluster_ids']))
            print(f"""
      - test_cluster_ids_exist:
          cluster_ids: "{cluster_ids_str}\"""")

        if model['needs_bnf_test'] and not model['has_bnf_test']:
            bnf_codes_str = ", ".join(sorted(model['bnf_codes']))
            print(f"""
      - test_bnf_codes_exist:
          bnf_codes: "{bnf_codes_str}\"""")


def fix_misplaced_tests() -> None:
    """Fix tests that were incorrectly placed in the columns section."""
    models_dir = Path('models')
    yaml_files = list(models_dir.rglob('*.yml'))

    files_fixed = 0

    for yaml_file in yaml_files:
        try:
            with open(yaml_file, 'r', encoding='utf-8') as f:
                content = f.read()

            # Look for test_cluster_ids_exist or test_bnf_codes_exist in columns section
            lines = content.split('\n')
            misplaced_tests = []
            lines_to_remove = []
            in_columns = False

            for i, line in enumerate(lines):
                if line.strip().startswith('columns:'):
                    in_columns = True
                    continue
                elif in_columns and line.strip() and not line.startswith('      '):
                    # We've left the columns section
                    in_columns = False

                if in_columns and ('test_cluster_ids_exist:' in line or 'test_bnf_codes_exist:' in line):
                    # Found a misplaced test
                    test_lines = [line]
                    lines_to_remove.append(i)

                    # Get the next line (parameters)
                    if i + 1 < len(lines) and ('cluster_ids:' in lines[i + 1] or 'bnf_codes:' in lines[i + 1]):
                        test_lines.append(lines[i + 1])
                        lines_to_remove.append(i + 1)

                    misplaced_tests.append(test_lines)

            if misplaced_tests:
                print(f"Fixing {yaml_file.name} - found {len(misplaced_tests)} misplaced test(s)")

                # Remove misplaced tests
                for line_idx in reversed(lines_to_remove):
                    lines.pop(line_idx)

                # Add tests at the correct location (after columns section)
                fixed_content = '\n'.join(lines)

                # Find where to insert model-level tests
                model_name = yaml_file.stem
                insert_pos = len(lines)

                # Look for end of columns section
                for i, line in enumerate(lines):
                    if f"- name: {model_name}" in line:
                        # Found our model, now look for end of columns
                        for j in range(i + 1, len(lines)):
                            if (lines[j].strip() and
                                not lines[j].startswith('      ') and
                                (lines[j].strip().startswith('- name:') or
                                 (len(lines[j]) - len(lines[j].lstrip()) <= 4 and lines[j].strip()))):
                                insert_pos = j
                                break
                        break

                # Build the tests section
                test_lines = ['', '    tests:']
                for test_group in misplaced_tests:
                    for test_line in test_group:
                        # Adjust indentation (change from column-level to model-level)
                        adjusted_line = '  ' + test_line.lstrip()
                        test_lines.append(adjusted_line)

                # Insert the tests
                lines[insert_pos:insert_pos] = test_lines

                # Write back to file
                with open(yaml_file, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(lines))

                files_fixed += 1

        except Exception as e:
            print(f"❌ Error processing {yaml_file}: {e}")

    print(f"\n✅ Fixed {files_fixed} YAML files with misplaced tests")


def main():
    """Main function to run the analysis."""
    import argparse

    parser = argparse.ArgumentParser(description='Flag models using get_observations/get_medication_orders macros')
    parser.add_argument('--issues-only', action='store_true',
                       help='Only show models that need attention (missing tests or YAML files)')
    parser.add_argument('--apply-tests', action='store_true',
                       help='Automatically add missing tests to YAML files')
    parser.add_argument('--dry-run', action='store_true', default=True,
                       help='Show what would be changed without making changes (default)')
    parser.add_argument('--fix-misplaced', action='store_true',
                       help='Fix tests that were incorrectly placed in columns section')
    args = parser.parse_args()

    # Handle fix-misplaced option first
    if args.fix_misplaced:
        print("Fixing misplaced tests in YAML files...")
        fix_misplaced_tests()
        return

    # Handle dry-run logic
    if args.apply_tests:
        dry_run = False
    else:
        dry_run = True

    # Find the models directory
    models_dir = Path('models')
    if not models_dir.exists():
        print("Error: models directory not found. Run this script from the dbt project root.")
        sys.exit(1)

    print("Analysing models for get_observations and get_medication_orders macro usage...")

    # Find all models using the macros
    models = find_models_using_macros(models_dir)

    if not models:
        print("No models found using get_observations or get_medication_orders macros.")
        return

    # Categorise models by test status
    categories = categorise_models(models)

    if args.issues_only:
        # Filter to show only categories with issues
        filtered_categories = {
            category: models_list for category, models_list in categories.items()
            if category in ['missing_yaml', 'missing_cluster_test', 'missing_bnf_test']
        }
        print_summary(filtered_categories)

        # Handle test addition
        models_needing_tests = (filtered_categories.get('missing_cluster_test', []) +
                               filtered_categories.get('missing_bnf_test', []))

        if models_needing_tests:
            print(f"\n{'='*80}")
            print("AUTOMATIC TEST ADDITION")
            print(f"{'='*80}")
            add_missing_tests_to_yaml(models_needing_tests, dry_run=dry_run)

        if dry_run and models_needing_tests:
            generate_test_snippets(filtered_categories)
    else:
        # Print summary
        print_summary(categories)

        # Handle test addition
        models_needing_tests = (categories.get('missing_cluster_test', []) +
                               categories.get('missing_bnf_test', []))

        if models_needing_tests:
            print(f"\n{'='*80}")
            print("AUTOMATIC TEST ADDITION")
            print(f"{'='*80}")
            add_missing_tests_to_yaml(models_needing_tests, dry_run=dry_run)

        if dry_run and models_needing_tests:
            # Generate test snippets
            generate_test_snippets(categories)


if __name__ == '__main__':
    main()
