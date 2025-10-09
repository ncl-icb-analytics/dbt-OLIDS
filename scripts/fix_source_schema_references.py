"""
Fix source schema references in extracted base models.

Tables in OLIDS_COMMON should use source('olids_common', ...)
Tables in OLIDS_MASKED should use source('olids_masked', ...)
"""

from pathlib import Path
import re

# Tables that should be in OLIDS_COMMON (from earlier sources.yml split)
COMMON_TABLES = {
    'ALLERGY_INTOLERANCE',
    'APPOINTMENT',
    'APPOINTMENT_PRACTITIONER',
    'DIAGNOSTIC_ORDER',
    'ENCOUNTER',
    'EPISODE_OF_CARE',
    'FLAG',
    'LOCATION',
    'LOCATION_CONTACT',
    'MEDICATION_ORDER',
    'MEDICATION_STATEMENT',
    'OBSERVATION',
    'ORGANISATION',
    'PATIENT_PERSON',
    'PATIENT_REGISTERED_PRACTITIONER_IN_ROLE',
    'PRACTITIONER',
    'PRACTITIONER_IN_ROLE',
    'PROCEDURE_REQUEST',
    'REFERRAL_REQUEST',
    'SCHEDULE',
    'SCHEDULE_PRACTITIONER'
}

def fix_source_reference(file_path: Path) -> bool:
    """
    Fix source reference in a single file if needed.

    Returns:
        True if file was modified, False otherwise
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Extract table name from the source() call
    match = re.search(r"source\('olids_masked',\s*'([A-Z_]+)'\)", content)
    if not match:
        return False

    table_name = match.group(1)

    # Check if this table should be in olids_common
    if table_name in COMMON_TABLES:
        new_content = content.replace(
            f"source('olids_masked', '{table_name}')",
            f"source('olids_common', '{table_name}')"
        )

        if new_content != content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
            return True

    return False


def main():
    """Main execution."""
    base_dir = Path('models/olids/base')
    sql_files = list(base_dir.glob('base_olids_*.sql'))

    print("Fixing source schema references...\n")

    updated_count = 0

    for sql_file in sql_files:
        if fix_source_reference(sql_file):
            print(f"[OK] Fixed: {sql_file.name}")
            updated_count += 1

    print(f"\n{updated_count} files updated to use olids_common")


if __name__ == '__main__':
    main()
