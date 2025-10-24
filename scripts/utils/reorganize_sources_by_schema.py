"""
Reorganize sources.yml to split olids_core into OLIDS_MASKED and OLIDS_COMMON sources.

OLIDS_MASKED: Tables containing patient/person data
OLIDS_COMMON: Reference tables without patient data
"""

import yaml
from pathlib import Path

# Tables that should stay in OLIDS_MASKED (only core patient/person entities)
MASKED_TABLES = {
    'PATIENT',
    'PATIENT_ADDRESS',
    'PATIENT_CONTACT',
    'PATIENT_UPRN',
}

# All other tables should go to OLIDS_COMMON

def reorganize_sources():
    sources_path = Path('C:/projects/dbt-olids/models/sources.yml')

    # Read the current sources.yml
    with open(sources_path, 'r') as f:
        data = yaml.safe_load(f)

    # Find the olids_core source
    olids_core_index = None
    for i, source in enumerate(data['sources']):
        if source['name'] == 'olids_core':
            olids_core_index = i
            break

    if olids_core_index is None:
        print("ERROR: olids_core source not found")
        return

    olids_core = data['sources'][olids_core_index]

    # Split tables into MASKED and COMMON
    masked_tables = []
    common_tables = []

    for table in olids_core['tables']:
        if table['name'] in MASKED_TABLES:
            masked_tables.append(table)
        else:
            common_tables.append(table)

    # Create new OLIDS_MASKED source
    olids_masked = {
        'name': 'olids_masked',
        'database': '"Data_Store_OLIDS_Alpha"',
        'schema': '"OLIDS_MASKED"',
        'description': 'OLIDS patient entity data (masked)',
        'tables': masked_tables
    }

    # Create new OLIDS_COMMON source
    olids_common = {
        'name': 'olids_common',
        'database': '"Data_Store_OLIDS_Alpha"',
        'schema': '"OLIDS_COMMON"',
        'description': 'OLIDS clinical events and reference data',
        'tables': common_tables
    }

    # Replace olids_core with the two new sources
    data['sources'][olids_core_index] = olids_masked
    data['sources'].insert(olids_core_index + 1, olids_common)

    # Write back to sources.yml
    with open(sources_path, 'w') as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False, width=120)

    print(f"✓ Split olids_core into two sources:")
    print(f"  - olids_masked: {len(masked_tables)} tables")
    print(f"  - olids_common: {len(common_tables)} tables")
    print(f"\nOLIDS_COMMON tables:")
    for table in sorted([t['name'] for t in common_tables]):
        print(f"    - {table}")

if __name__ == '__main__':
    reorganize_sources()
