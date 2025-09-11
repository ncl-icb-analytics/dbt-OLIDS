#!/usr/bin/env python3
"""
Generate schema.yml for base layer models
Creates schema file with basic tests for base layer views
"""

import yaml
import os
import sys
from pathlib import Path

# Path configuration
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = Path(CURRENT_DIR).parent.parent
BASE_DIR = PROJECT_DIR / 'models' / 'olids' / 'base'
SCHEMA_FILE = BASE_DIR / 'schema.yml'

def get_model_description(model_name, table_name):
    """Generate detailed description based on filtering pattern applied to the model"""
    
    # Clinical models (have both patient and organisation filtering)
    clinical_tables = ['OBSERVATION', 'ENCOUNTER', 'APPOINTMENT', 'MEDICATION_ORDER', 'MEDICATION_STATEMENT', 
                      'PROCEDURE_REQUEST', 'ALLERGY_INTOLERANCE', 'DIAGNOSTIC_ORDER', 'EPISODE_OF_CARE', 
                      'FLAG', 'REFERRAL_REQUEST', 'PATIENT_ADDRESS', 'PATIENT_CONTACT', 
                      'PATIENT_REGISTERED_PRACTITIONER_IN_ROLE']
    
    # Infrastructure models (organisation filtering only)
    infrastructure_tables = ['PRACTITIONER', 'PRACTITIONER_IN_ROLE', 'SCHEDULE', 'SCHEDULE_PRACTITIONER', 
                           'APPOINTMENT_PRACTITIONER']
    
    # Reference models (no filtering)
    reference_tables = ['LOCATION', 'LOCATION_CONTACT', 'ORGANISATION', 'PATIENT_UPRN']
    
    # Generated models (special handling)
    generated_tables = ['PATIENT_PERSON', 'PERSON']
    
    # Patient base model
    if table_name == 'PATIENT':
        return """Base filtered patient view. Foundation for all patient-related filtering.

Excludes sensitive patients:
- is_spine_sensitive = FALSE (excludes spine-sensitive patients)
- is_confidential = FALSE (excludes confidential patients) 
- is_dummy_patient = FALSE (excludes test/dummy patients)

Restricts to NCL practices:
- record_owner_organisation_code IN (NCL practice codes with STPCode = 'QMJ')"""
    
    elif table_name in clinical_tables:
        return f"""Filtered {table_name.replace('_', ' ').title()} base view.

Applied filters:
- Patient filtering: Excludes sensitive patients (is_spine_sensitive=FALSE, is_confidential=FALSE, is_dummy_patient=FALSE)
- Practice filtering: Restricts to NCL practices only (STPCode = 'QMJ')
- Data integrity: Inner joins ensure only records with valid patient and organisation references

Filtering method: Inner join to base_olids_patient and int_ncl_practices"""
        
    elif table_name in infrastructure_tables:
        return f"""Filtered {table_name.replace('_', ' ').title()} base view.

Applied filters:
- Practice filtering: Restricts to NCL practices only (STPCode = 'QMJ')
- Data integrity: Inner join ensures only records with valid organisation references

Filtering method: Inner join to int_ncl_practices on record_owner_organisation_code"""
        
    elif table_name in reference_tables:
        return f"""Unfiltered {table_name.replace('_', ' ').title()} reference view.

No filters applied - reference data used as-is.
Direct passthrough from source table with explicit column selection for interface consistency."""
        
    elif table_name in generated_tables:
        return f"""Generated {table_name.replace('_', ' ').title()} base view.

Applied filters:
- Patient filtering: Inherits filtering from underlying patient relationships
- Practice filtering: Inherits NCL practice restriction through patient associations

Filtering method: Filtered through relationships to base_olids_patient"""
        
    else:
        # Fallback for any models not explicitly categorised
        return f"""Filtered {table_name.replace('_', ' ').title()} base view.

Applied filters: Restricts data to NCL practices and excludes sensitive patients.
Filtering method varies based on table structure and available foreign keys."""

def generate_base_schema():
    """Generate schema.yml for base layer models"""
    
    # Get all base model files
    if not BASE_DIR.exists():
        print(f"ERROR: Base directory not found at {BASE_DIR}")
        return False
    
    base_files = list(BASE_DIR.glob('base_olids_*.sql'))
    if not base_files:
        print(f"ERROR: No base models found in {BASE_DIR}")
        return False
    
    # Create schema structure
    schema_data = {
        'version': 2,
        'models': []
    }
    
    # Add models to schema
    for file_path in sorted(base_files):
        model_name = file_path.stem  # Remove .sql extension
        table_name = model_name.replace('base_olids_', '').upper()
        
        # Determine description based on filtering pattern
        description = get_model_description(model_name, table_name)
        
        # Simple model definition with just description
        model_def = {
            'name': model_name,
            'description': description
        }
        
        schema_data['models'].append(model_def)
    
    # Write schema file
    with open(SCHEMA_FILE, 'w') as f:
        yaml.dump(schema_data, f, default_flow_style=False, sort_keys=False, width=120)
    
    print(f"SUCCESS: Generated base layer schema with {len(schema_data['models'])} models")
    print(f"Schema file: {SCHEMA_FILE}")
    
    return True

if __name__ == "__main__":
    print("Generating base layer schema.yml...")
    
    try:
        success = generate_base_schema()
        if success:
            print("\nBase layer schema generation complete!")
        else:
            print("\nBase layer schema generation failed!")
            sys.exit(1)
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)