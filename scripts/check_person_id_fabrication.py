"""
Check person_id fabrication is correctly applied across base models.
"""

from pathlib import Path
import re

def check_model(file_path: Path) -> dict:
    """Check if a model correctly handles person_id."""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    has_patient_id = 'patient_id' in content.lower()
    has_person_id_col = re.search(r'(pp\.person_id|src\."person_id")', content)
    has_patient_person_join = 'base_olids_patient_person' in content

    return {
        'file': file_path.name,
        'has_patient_id': has_patient_id,
        'has_person_id_col': bool(has_person_id_col),
        'has_patient_person_join': has_patient_person_join,
        'ok': not has_patient_id or (has_person_id_col and has_patient_person_join)
    }


def main():
    """Main execution."""
    base_dir = Path('models/olids/base')
    results = []

    for sql_file in sorted(base_dir.glob('base_olids_*.sql')):
        # Skip ncl_practices and terminology models
        if 'ncl_practices' in sql_file.name or 'terminology' in sql_file.name:
            continue

        result = check_model(sql_file)
        results.append(result)

    # Report issues
    issues = [r for r in results if not r['ok']]
    good = [r for r in results if r['ok'] and r['has_patient_id']]
    no_patient = [r for r in results if not r['has_patient_id']]

    print("=== Models with patient_id and correct person_id fabrication ===")
    for r in good:
        print(f"  [OK] {r['file']}")

    print(f"\n=== Models without patient_id (reference tables) ===")
    for r in no_patient:
        print(f"  [REF] {r['file']}")

    if issues:
        print(f"\n=== ISSUES: Models with patient_id but missing person_id fabrication ===")
        for r in issues:
            print(f"  [ERROR] {r['file']}")
            if not r['has_person_id_col']:
                print(f"    - Missing: pp.person_id in SELECT")
            if not r['has_patient_person_join']:
                print(f"    - Missing: INNER JOIN to base_olids_patient_person")

    print(f"\n=== Summary ===")
    print(f"  Total models checked: {len(results)}")
    print(f"  With patient_id (correct): {len(good)}")
    print(f"  Without patient_id (reference): {len(no_patient)}")
    print(f"  Issues found: {len(issues)}")


if __name__ == '__main__':
    main()
