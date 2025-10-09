"""
Restructure sources.yml to split OLIDS_MASKED and OLIDS_COMMON schemas.

This script:
1. Reads the current sources.yml
2. Splits olids_core into two sources:
   - olids_masked: Contains patient/person tables
   - olids_common: Contains all other OLIDS tables
3. Writes the updated sources.yml
"""

import yaml
from pathlib import Path
from typing import Any, Dict, List

# Tables that should remain in OLIDS_MASKED
MASKED_TABLES = {
    'PATIENT',
    'PERSON',
    'PATIENT_UPRN',
    'PATIENT_CONTACT',
    'PATIENT_ADDRESS'
}


def load_sources(file_path: Path) -> Dict[str, Any]:
    """Load sources.yml file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def save_sources(data: Dict[str, Any], file_path: Path) -> None:
    """Save sources.yml file with proper formatting."""
    with open(file_path, 'w', encoding='utf-8') as f:
        yaml.dump(data, f, sort_keys=False, allow_unicode=True, width=1000)


def restructure_olids_sources(sources_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Split olids_core source into olids_masked and olids_common.

    Args:
        sources_data: Parsed sources.yml data

    Returns:
        Updated sources data with split schemas
    """
    sources_list = sources_data['sources']

    # Find olids_core source
    olids_core_idx = None
    olids_core = None

    for idx, source in enumerate(sources_list):
        if source['name'] == 'olids_core':
            olids_core_idx = idx
            olids_core = source
            break

    if not olids_core:
        raise ValueError("olids_core source not found in sources.yml")

    # Separate tables into masked and common
    masked_tables = []
    common_tables = []

    for table in olids_core['tables']:
        if table['name'] in MASKED_TABLES:
            masked_tables.append(table)
        else:
            common_tables.append(table)

    # Create new sources
    olids_masked = {
        'name': 'olids_masked',
        'database': '"Data_Store_OLIDS_Alpha"',
        'schema': '"OLIDS_MASKED"',
        'description': 'OLIDS patient and person data',
        'tables': masked_tables
    }

    olids_common = {
        'name': 'olids_common',
        'database': '"Data_Store_OLIDS_Alpha"',
        'schema': '"OLIDS_COMMON"',
        'description': 'OLIDS clinical and organisational data',
        'tables': common_tables
    }

    # Replace olids_core with the two new sources
    sources_list[olids_core_idx] = olids_masked
    sources_list.insert(olids_core_idx + 1, olids_common)

    return sources_data


def main():
    """Main execution."""
    sources_path = Path('models/sources.yml')
    backup_path = Path('models/sources.yml.backup')

    print(f"Loading {sources_path}...")
    sources_data = load_sources(sources_path)

    print("Creating backup...")
    save_sources(sources_data, backup_path)
    print(f"Backup created at {backup_path}")

    print("\nRestructuring sources...")
    updated_data = restructure_olids_sources(sources_data)

    print("Saving updated sources.yml...")
    save_sources(updated_data, sources_path)

    print("\n✓ sources.yml has been restructured successfully!")
    print("\nChanges made:")
    print("  - Split olids_core into olids_masked and olids_common")
    print("  - olids_masked contains: PATIENT, PERSON, PATIENT_UPRN, PATIENT_CONTACT, PATIENT_ADDRESS")
    print("  - olids_common contains: All other OLIDS tables")
    print(f"\nOriginal file backed up to: {backup_path}")
    print("\nNext steps:")
    print("  1. Review the changes in sources.yml")
    print("  2. Update base and stable layer models to use new source names")
    print("  3. Run: python scripts/compare_sources_to_snowflake.py")


if __name__ == '__main__':
    main()
