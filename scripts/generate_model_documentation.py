"""
Generate comprehensive YAML documentation for base and stable models.

Extracts inline SQL comments and creates properly formatted schema.yml files.
"""

from pathlib import Path
import re
from typing import Dict, List, Optional
import yaml


def extract_model_description(sql_file: Path) -> Optional[str]:
    """Extract description from SQL file comments."""
    with open(sql_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # Look for comment block after config
    comment_match = re.search(r'/\*\s*(.*?)\s*\*/', content, re.DOTALL)
    if comment_match:
        comment_text = comment_match.group(1).strip()
        # Clean up the comment text
        lines = [line.strip() for line in comment_text.split('\n')]
        return '\n'.join(lines)

    return None


def generate_base_description(table_name: str, sql_content: str) -> str:
    """Generate description for base model based on its pattern."""
    has_patient_filter = 'base_olids_patient' in sql_content
    has_ncl_filter = 'base_olids_ncl_practices' in sql_content
    has_person_id_fabrication = 'base_olids_patient_person' in sql_content

    if has_patient_filter and has_ncl_filter:
        desc = f"Filtered {table_name.replace('_', ' ').title()} base view.\n\n"
        desc += "Applied filters:\n"
        desc += "- Patient filtering: Excludes sensitive patients (is_spine_sensitive=FALSE, is_confidential=FALSE, is_dummy_patient=FALSE)\n"
        desc += "- Practice filtering: Restricts to NCL practices only (STPCode='QMJ')\n"
        desc += "- Data integrity: Inner joins ensure only records with valid patient and organisation references\n\n"

        if has_person_id_fabrication:
            desc += "Note: person_id is replaced with fabricated version from patient_person mapping\n\n"

        desc += "Filtering method: Inner join to base_olids_patient and base_ncl_practices"

    elif has_ncl_filter and not has_patient_filter:
        desc = f"Filtered {table_name.replace('_', ' ').title()} base view.\n\n"
        desc += "Applied filters:\n"
        desc += "- Practice filtering: Restricts to NCL practices only (STPCode='QMJ')\n"
        desc += "- Data integrity: Inner join ensures only records with valid organisation references\n\n"
        desc += "Filtering method: Inner join to base_ncl_practices on record_owner_organisation_code"

    else:
        desc = f"Unfiltered {table_name.replace('_', ' ').title()} base view.\n\n"
        desc += "No filters applied - reference data used as-is.\n"
        desc += "Direct passthrough from source table with explicit column selection for interface consistency."

    return desc


def generate_stable_description(table_name: str) -> str:
    """Generate description for stable incremental model."""
    # Determine table type
    clinical_tables = [
        'observation', 'medication_order', 'medication_statement',
        'allergy_intolerance', 'diagnostic_order', 'procedure_request',
        'referral_request', 'encounter', 'flag', 'appointment'
    ]

    entity_tables = ['patient', 'person', 'practitioner', 'organisation', 'location']

    reference_tables = [
        'practitioner_in_role', 'appointment_practitioner',
        'schedule', 'schedule_practitioner', 'location_contact',
        'patient_contact', 'patient_address', 'patient_uprn'
    ]

    table_display = table_name.replace('_', ' ').title()

    if table_name in clinical_tables:
        desc = f"Incremental {table_display.lower()} table.\n\n"
        desc += "Clinical event records with NCL patient filtering and quality controls applied.\n"
        desc += "Uses merge strategy with clustering on source concept and clinical effective date for optimal query performance."

    elif table_name in entity_tables:
        desc = f"Incremental {table_display.lower()} entity table.\n\n"
        desc += f"Core {table_display.lower()} demographics and attributes.\n"
        if table_name == 'patient':
            desc += "NCL filtering applied with sensitive patients excluded."
        else:
            desc += "Provides stable interface for downstream analytical models."

    elif table_name in reference_tables:
        desc = f"Incremental {table_display.lower()} reference table.\n\n"
        desc += f"{table_display} relationships and attributes.\n"
        desc += "Maintains referential integrity with parent entities."

    else:
        desc = f"Incremental {table_display.lower()} table.\n\n"
        desc += "Provides stable interface between source data and analytical models.\n"
        desc += "Uses incremental materialisation for efficient updates."

    return desc


def generate_schema_yml(model_dir: Path, is_stable: bool = False) -> Dict:
    """Generate schema.yml content for models in directory."""
    models = []

    sql_files = sorted(model_dir.glob('*.sql'))

    for sql_file in sql_files:
        # Skip non-model files
        if not (sql_file.stem.startswith('base_olids_') or sql_file.stem.startswith('stable_')):
            continue

        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()

        # Extract table name
        if is_stable:
            table_name = sql_file.stem.replace('stable_', '')
            description = generate_stable_description(table_name)
        else:
            table_name = sql_file.stem.replace('base_olids_', '')
            description = generate_base_description(table_name, sql_content)

        models.append({
            'name': sql_file.stem,
            'description': description
        })

    return {
        'version': 2,
        'models': models
    }


def main():
    """Main execution."""
    # Generate base schema.yml
    base_dir = Path('models/olids/base')
    base_schema = generate_schema_yml(base_dir, is_stable=False)

    base_schema_path = base_dir / 'schema.yml'
    with open(base_schema_path, 'w', encoding='utf-8') as f:
        yaml.dump(base_schema, f, sort_keys=False, allow_unicode=True, width=1000, default_flow_style=False)

    print(f"[OK] Generated {base_schema_path}")
    print(f"     {len(base_schema['models'])} base models documented")

    # Generate stable schema.yml
    stable_dir = Path('models/olids/stable')
    stable_schema = generate_schema_yml(stable_dir, is_stable=True)

    stable_schema_path = stable_dir / 'schema.yml'
    with open(stable_schema_path, 'w', encoding='utf-8') as f:
        yaml.dump(stable_schema, f, sort_keys=False, allow_unicode=True, width=1000, default_flow_style=False)

    print(f"[OK] Generated {stable_schema_path}")
    print(f"     {len(stable_schema['models'])} stable models documented")

    print(f"\nDocumentation generated successfully!")
    print(f"Total models documented: {len(base_schema['models']) + len(stable_schema['models'])}")


if __name__ == '__main__':
    main()
