"""
Update source references from olids_core to olids_masked/olids_common.

This script updates all dbt model files to reference the correct new source:
- olids_masked for: PATIENT, PERSON, PATIENT_UPRN, PATIENT_CONTACT, PATIENT_ADDRESS
- olids_common for: all other OLIDS tables
"""

from pathlib import Path
from typing import Dict, Set
import re

# Tables that should use olids_masked source
MASKED_TABLES = {
    'PATIENT',
    'PERSON',
    'PATIENT_UPRN',
    'PATIENT_CONTACT',
    'PATIENT_ADDRESS'
}


def get_table_name_from_source_call(line: str) -> str | None:
    """
    Extract table name from a source() function call.

    Args:
        line: Line of SQL containing source() call

    Returns:
        Table name or None if not found
    """
    pattern = r"source\s*\(\s*['\"]olids_core['\"]\s*,\s*['\"]([A-Z_]+)['\"]\s*\)"
    match = re.search(pattern, line)
    if match:
        return match.group(1)
    return None


def determine_new_source(table_name: str) -> str:
    """
    Determine which source a table should use.

    Args:
        table_name: Name of the table

    Returns:
        'olids_masked' or 'olids_common'
    """
    return 'olids_masked' if table_name in MASKED_TABLES else 'olids_common'


def update_file(file_path: Path) -> Dict[str, str]:
    """
    Update source references in a file.

    Args:
        file_path: Path to file to update

    Returns:
        Dict with old and new source names
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    original_content = content
    changes = {}

    # Find all source('olids_core', 'TABLE_NAME') calls
    pattern = r"source\s*\(\s*['\"]olids_core['\"]\s*,\s*['\"]([A-Z_]+)['\"]\s*\)"

    def replace_source(match):
        table_name = match.group(1)
        new_source = determine_new_source(table_name)
        changes[table_name] = new_source
        return f"source('{new_source}', '{table_name}')"

    content = re.sub(pattern, replace_source, content)

    # Only write if changes were made
    if content != original_content:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)

    return changes


def main():
    """Main execution."""
    base_path = Path('models/olids')

    # Find all SQL files that might reference olids_core
    sql_files = list(base_path.rglob('*.sql'))

    print("Updating source references from olids_core to olids_masked/olids_common...\n")

    files_updated = 0
    total_changes = {}

    for sql_file in sql_files:
        with open(sql_file, 'r', encoding='utf-8') as f:
            content = f.read()

        if "source('olids_core'" in content or 'source("olids_core"' in content:
            changes = update_file(sql_file)
            if changes:
                files_updated += 1
                print(f"Updated: {sql_file}")
                for table, new_source in changes.items():
                    print(f"  {table} -> {new_source}")
                    total_changes[table] = new_source

    print(f"\n{files_updated} files updated successfully!")

    if total_changes:
        print("\nSummary of changes:")
        masked_tables = [t for t, s in total_changes.items() if s == 'olids_masked']
        common_tables = [t for t, s in total_changes.items() if s == 'olids_common']

        if masked_tables:
            print(f"\nMoved to olids_masked ({len(masked_tables)} tables):")
            for table in sorted(masked_tables):
                print(f"  - {table}")

        if common_tables:
            print(f"\nMoved to olids_common ({len(common_tables)} tables):")
            for table in sorted(common_tables):
                print(f"  - {table}")


if __name__ == '__main__':
    main()
