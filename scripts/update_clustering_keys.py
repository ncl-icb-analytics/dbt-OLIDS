"""
Update clustering keys for stable models to match their natural grain.
"""

from pathlib import Path
import re

# Define appropriate clustering for each table based on natural grain
CLUSTERING_CONFIG = {
    # Clinical event tables - cluster by source_concept_id, clinical_effective_date
    'observation': ['observation_source_concept_id', 'clinical_effective_date'],
    'medication_order': ['medication_order_source_concept_id', 'clinical_effective_date'],
    'medication_statement': ['medication_statement_source_concept_id', 'clinical_effective_date'],
    'allergy_intolerance': ['allergy_intolerance_source_concept_id', 'clinical_effective_date'],
    'diagnostic_order': ['diagnostic_order_source_concept_id', 'clinical_effective_date'],
    'procedure_request': ['procedure_request_source_concept_id', 'clinical_effective_date'],
    'referral_request': ['referral_request_source_concept_id', 'clinical_effective_date'],
    'encounter': ['encounter_source_concept_id', 'clinical_effective_date'],
    'flag': ['flag_source_concept_id', 'clinical_effective_date'],

    # Appointment - natural grain is start_date, patient_id
    'appointment': ['start_date', 'patient_id'],

    # Episode of care - natural grain is registration_type_concept_id, date_registered
    'episode_of_care': ['registration_type_concept_id', 'date_registered'],

    # Entity tables - cluster by primary key
    'patient': ['id'],
    'person': ['id'],
    'organisation': ['id'],
    'location': ['id'],
    'practitioner': ['id'],

    # Patient-related reference tables - cluster by patient_id
    'patient_address': ['patient_id', 'effective_from'],
    'patient_contact': ['patient_id'],
    'patient_uprn': ['patient_id'],
    'patient_registered_practitioner_in_role': ['patient_id', 'start_date'],

    # Mapping table
    'patient_person': ['patient_id', 'person_id'],

    # Infrastructure tables
    'practitioner_in_role': ['practitioner_id', 'organisation_id'],
    'appointment_practitioner': ['appointment_id', 'practitioner_id'],
    'schedule': ['id'],
    'schedule_practitioner': ['schedule_id', 'practitioner_id'],
    'location_contact': ['location_id'],

    # Reference tables
    'postcode_hash': ['postcode_hash'],
    'terminology_concept': ['id'],
    'terminology_concept_map': ['source_code_id', 'target_code_id'],
}


def update_clustering(file_path: Path) -> bool:
    """Update clustering configuration in stable model."""
    table_name = file_path.stem.replace('stable_', '')

    if table_name not in CLUSTERING_CONFIG:
        print(f"[SKIP] {table_name} - No clustering config defined")
        return False

    cluster_keys = CLUSTERING_CONFIG[table_name]

    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Find and replace cluster_by configuration
    cluster_str = ", ".join([f"'{k}'" for k in cluster_keys])
    new_cluster_line = f"        cluster_by=[{cluster_str}],"

    # Replace existing cluster_by line
    content = re.sub(
        r'        cluster_by=\[.*?\],',
        new_cluster_line,
        content
    )

    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)

    return True


def main():
    """Main execution."""
    stable_dir = Path('models/olids/stable')

    print("Updating clustering keys for stable models...\n")

    updated = []
    skipped = []

    for sql_file in sorted(stable_dir.glob('stable_*.sql')):
        table_name = sql_file.stem.replace('stable_', '')

        if update_clustering(sql_file):
            cluster_keys = CLUSTERING_CONFIG[table_name]
            updated.append((table_name, cluster_keys))
            print(f"[OK] {table_name}: {cluster_keys}")
        else:
            skipped.append(table_name)

    print(f"\n{'='*60}")
    print(f"Updated {len(updated)} stable models")

    if skipped:
        print(f"Skipped {len(skipped)} models (no config)")


if __name__ == '__main__':
    main()
