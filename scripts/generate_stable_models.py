"""
Generate missing stable incremental models from base models.

Reads base model DDL and creates corresponding stable incremental table definitions.
"""

from pathlib import Path
import re
from typing import List, Tuple, Optional


def extract_columns_from_base(base_model_path: Path) -> List[str]:
    """Extract column list from base model SELECT statement."""
    with open(base_model_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Find the SELECT section
    select_match = re.search(r'SELECT\s+(.*?)\s+FROM', content, re.IGNORECASE | re.DOTALL)
    if not select_match:
        raise ValueError(f"Could not find SELECT statement in {base_model_path}")

    select_section = select_match.group(1)

    # Extract column aliases (everything after AS)
    columns = []
    for line in select_section.split('\n'):
        line = line.strip()
        if not line or line.startswith('--') or line.startswith('/*'):
            continue

        # Match: something AS column_name
        match = re.search(r'\bAS\s+([a-z_]+)', line, re.IGNORECASE)
        if match:
            col_name = match.group(1)
            # Remove trailing comma if present
            col_name = col_name.rstrip(',')
            columns.append(col_name)

    return columns


def determine_cluster_by(table_name: str, columns: List[str]) -> Optional[List[str]]:
    """Determine appropriate clustering columns based on table type."""
    # Clinical event tables - cluster by source concept and effective date
    clinical_tables = {
        'observation': ['observation_source_concept_id', 'clinical_effective_date'],
        'medication_order': ['medication_order_source_concept_id', 'clinical_effective_date'],
        'medication_statement': ['medication_statement_source_concept_id', 'clinical_effective_date'],
        'allergy_intolerance': ['allergy_intolerance_source_concept_id', 'clinical_effective_date'],
        'diagnostic_order': ['diagnostic_order_source_concept_id', 'clinical_effective_date'],
        'procedure_request': ['procedure_request_source_concept_id', 'clinical_effective_date'],
        'referral_request': ['referral_request_source_concept_id', 'clinical_effective_date'],
        'encounter': ['encounter_source_concept_id', 'clinical_effective_date'],
        'flag': ['flag_source_concept_id', 'clinical_effective_date'],
    }

    # Appointment/scheduling tables
    if table_name in ['appointment', 'schedule']:
        return ['start_date']

    # Entity tables
    if table_name in ['patient', 'person', 'practitioner', 'organisation', 'location']:
        return None  # No clustering for entity tables

    # Check clinical tables
    if table_name in clinical_tables:
        # Verify columns exist
        cluster_cols = clinical_tables[table_name]
        if all(col in columns for col in cluster_cols):
            return cluster_cols

    return None


def create_stable_model(base_model_path: Path, output_dir: Path) -> str:
    """Create stable model from base model."""
    # Extract table name
    base_name = base_model_path.stem  # e.g., base_olids_allergy_intolerance
    table_name = base_name.replace('base_olids_', '')  # e.g., allergy_intolerance

    # Extract columns
    columns = extract_columns_from_base(base_model_path)

    # Determine clustering
    cluster_by = determine_cluster_by(table_name, columns)

    # Build config
    config_lines = [
        "{{",
        "    config(",
        "        materialized='incremental',",
        "        unique_key='id',",
        "        on_schema_change='fail',",
    ]

    if cluster_by:
        cluster_str = ", ".join([f"'{col}'" for col in cluster_by])
        config_lines.append(f"        cluster_by=[{cluster_str}],")

    config_lines.extend([
        f"        alias='{table_name}',",
        "        incremental_strategy='merge',",
        "        tags=['stable', 'incremental']",
        "    )",
        "}}"
    ])

    # Build SELECT
    select_lines = ["", "select"]
    for i, col in enumerate(columns):
        comma = "," if i < len(columns) - 1 else ""
        select_lines.append(f"    {col}{comma}")

    # Add FROM and incremental filter
    from_lines = [
        f"from {{{{ ref('{base_name}') }}}}",
        "",
        "{% if is_incremental() %}",
        "    where lds_start_date_time > (select max(lds_start_date_time) from {{ this }})",
        "{% endif %}"
    ]

    # Combine all parts
    model_content = "\n".join(config_lines + select_lines + from_lines)

    # Write file
    output_path = output_dir / f"stable_{table_name}.sql"
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(model_content)

    return output_path.name


def main():
    """Main execution."""
    base_dir = Path('models/olids/base')
    stable_dir = Path('models/olids/stable')
    stable_dir.mkdir(parents=True, exist_ok=True)

    # Get existing stable models
    existing_stable = {f.stem.replace('stable_', '') for f in stable_dir.glob('stable_*.sql')}

    # Tables to skip
    skip_tables = {
        'ncl_practices',  # Reference table, not needed in stable
        'concept',  # Covered by stable_terminology_concept
        'concept_map',  # Covered by stable_terminology_concept_map
        'postcode_hash',  # Derived model
    }

    print("Generating missing stable models...\n")

    created = []
    skipped = []

    for base_file in sorted(base_dir.glob('base_olids_*.sql')):
        table_name = base_file.stem.replace('base_olids_', '')

        # Skip if already exists or in skip list
        if table_name in existing_stable:
            continue

        if table_name in skip_tables:
            skipped.append(table_name)
            print(f"[SKIP] {table_name}")
            continue

        try:
            output_file = create_stable_model(base_file, stable_dir)
            created.append(table_name)
            print(f"[OK] Created: {output_file}")
        except Exception as e:
            print(f"[ERROR] Failed to create stable_{table_name}.sql: {str(e)}")

    print(f"\n{'='*60}")
    print(f"Generated {len(created)} stable models")
    print(f"Skipped {len(skipped)} tables")

    if created:
        print(f"\nCreated stable models:")
        for name in created:
            print(f"  - stable_{name}.sql")


if __name__ == '__main__':
    main()
