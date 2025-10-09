"""
Update base model SQL files to reference the correct source (olids_masked or olids_common).
"""

from pathlib import Path
import re

# Tables that should reference olids_masked (only core patient entities)
MASKED_TABLES = {
    'PATIENT',
    'PATIENT_ADDRESS',
    'PATIENT_CONTACT',
    'PATIENT_UPRN',
}

def update_base_models():
    base_dir = Path('C:/projects/dbt-olids/models/olids/base')

    updated_files = []

    for sql_file in base_dir.glob('base_olids_*.sql'):
        content = sql_file.read_text()

        # Check if file references olids_core or olids_masked
        if "source('olids_core'" not in content and "source('olids_masked'" not in content:
            continue

        # Determine which source to use based on the table name
        original_content = content

        # Check if this table should be in MASKED
        for table_name in MASKED_TABLES:
            pattern = rf"source\('olids_(core|masked|common)',\s*'{table_name}'\)"
            if re.search(pattern, content):
                content = re.sub(pattern, f"source('olids_masked', '{table_name}')", content)
                break

        # If not in MASKED_TABLES, change to olids_common
        if content == original_content:
            content = re.sub(r"source\('olids_(core|masked|common)'", "source('olids_common'", content)

        # Write back if changed
        if content != original_content:
            sql_file.write_text(content)
            updated_files.append(sql_file.name)
            print(f"Updated: {sql_file.name}")

    print(f"\nTotal files updated: {len(updated_files)}")

if __name__ == '__main__':
    update_base_models()
