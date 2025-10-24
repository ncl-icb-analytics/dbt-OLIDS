"""
Move tables between olids_masked and olids_common sources.
OLIDS_MASKED should only contain: PATIENT, PATIENT_ADDRESS, PATIENT_CONTACT, PATIENT_UPRN
All other tables should be in OLIDS_COMMON.
"""

import yaml
from pathlib import Path

MASKED_TABLES = {'PATIENT', 'PATIENT_ADDRESS', 'PATIENT_CONTACT', 'PATIENT_UPRN', 'PERSON'}

def fix_sources():
    sources_path = Path('C:/projects/dbt-olids/models/sources.yml')

    with open(sources_path, 'r') as f:
        data = yaml.safe_load(f)

    # Find both sources
    masked_idx = None
    common_idx = None

    for i, source in enumerate(data['sources']):
        if source['name'] == 'olids_masked':
            masked_idx = i
        elif source['name'] == 'olids_common':
            common_idx = i

    if masked_idx is None or common_idx is None:
        print("ERROR: Could not find both sources")
        return

    # Collect all tables and redistribute
    all_tables = (data['sources'][masked_idx]['tables'] +
                  data['sources'][common_idx]['tables'])

    masked_tables = [t for t in all_tables if t['name'] in MASKED_TABLES]
    common_tables = [t for t in all_tables if t['name'] not in MASKED_TABLES]

    # Update sources
    data['sources'][masked_idx]['tables'] = masked_tables
    data['sources'][masked_idx]['description'] = 'OLIDS patient entity data (masked)'

    data['sources'][common_idx]['tables'] = common_tables
    data['sources'][common_idx]['description'] = 'OLIDS clinical events and reference data'

    # Write back
    with open(sources_path, 'w') as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False, width=120)

    print(f"OLIDS_MASKED ({len(masked_tables)} tables):")
    for t in sorted([tb['name'] for tb in masked_tables]):
        print(f"  - {t}")

    print(f"\nOLIDS_COMMON ({len(common_tables)} tables):")
    for t in sorted([tb['name'] for tb in common_tables]):
        print(f"  - {t}")

if __name__ == '__main__':
    fix_sources()
