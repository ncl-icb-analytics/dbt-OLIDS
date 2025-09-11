import yaml
import os
import sys

# Path configuration
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(CURRENT_DIR)
PROJECT_DIR = os.path.dirname(SCRIPTS_DIR)
SOURCES_YML = os.path.join(PROJECT_DIR, 'models', 'sources.yml')
BASE_DIR = os.path.join(PROJECT_DIR, 'models', 'olids', 'base')
MAPPINGS_FILE = os.path.join(CURRENT_DIR, 'source_mappings.yml')

def load_sources():
    """Load sources.yml to get OLIDS table definitions"""
    with open(SOURCES_YML, 'r') as f:
        sources_data = yaml.safe_load(f)
    
    # Find olids_core source
    olids_tables = []
    for source in sources_data['sources']:
        if source['name'] == 'olids_core':
            for table in source['tables']:
                table_info = {
                    'name': table['name'],
                    'columns': [col['name'] for col in table.get('columns', [])]
                }
                olids_tables.append(table_info)
            break
    
    return olids_tables

def determine_filtering_pattern(table_name, columns):
    """Determine which filtering pattern to apply based on table structure"""
    has_patient_id = 'patient_id' in columns
    has_record_owner = 'record_owner_organisation_code' in columns
    
    # Special cases
    if table_name == 'PATIENT':
        return 'patient_base'  # Generate base_olids_patient with filtering logic
    elif table_name in ['PATIENT_PERSON', 'PERSON']:
        return 'generated'  # Special handling for generated tables
    elif table_name in ['LOCATION', 'LOCATION_CONTACT', 'ORGANISATION', 
                        'PATIENT_UPRN', 'CONCEPT', 'CONCEPT_MAP']:
        return 'reference'  # No filtering needed
    elif has_patient_id and has_record_owner:
        return 'clinical'  # Patient + organisation filtering
    elif has_record_owner:
        return 'infrastructure'  # Organisation filtering only
    else:
        return 'reference'  # No filtering needed

def generate_column_list(columns):
    """Generate explicit column list for SELECT statement"""
    if not columns:
        return "src.*"
    
    # Format columns with proper indentation
    formatted_columns = []
    for col in columns:
        formatted_columns.append(f'    src."{col}"')
    
    return ",\n".join(formatted_columns)

def generate_base_view_content(table_name, pattern, columns=None):
    """Generate the SQL content for a base view based on the filtering pattern"""
    
    # Use table name as alias (e.g., PATIENT, OBSERVATION, etc.)
    config_block = f'''{{{{
    config(
        secure=true,
        alias='{table_name.lower()}')
}}}}'''
    
    # Generate explicit column list
    column_list = generate_column_list(columns)
    
    if pattern == 'patient_base':
        return f'''{config_block}

/*
Base Filtered Patient View
Filters out sensitive patients and restricts to NCL practices only.
Used as the foundation for all patient-related clinical data filtering.

Exclusions:
- Patients with is_spine_sensitive = TRUE 
- Patients with is_confidential = TRUE
- Patients from non-NCL practices (where STPCode != 'QMJ')
*/

SELECT
{column_list}
FROM {{{{ source('olids_core', '{table_name}') }}}} src
INNER JOIN {{{{ ref('int_ncl_practices') }}}} ncl_practices
    ON src."record_owner_organisation_code" = ncl_practices.practice_code
WHERE src."is_spine_sensitive" = FALSE
    AND src."is_confidential" = FALSE
    AND src."is_dummy_patient" = FALSE'''
        
    elif pattern == 'clinical':
        return f'''{config_block}

/*
Base {table_name} View
Filters to NCL practices and excludes sensitive patients.
Pattern: Clinical table with patient_id + record_owner_organisation_code
*/

SELECT
{column_list}
FROM {{{{ source('olids_core', '{table_name}') }}}} src
INNER JOIN {{{{ ref('base_olids_patient') }}}} patients
    ON src."patient_id" = patients."id"
INNER JOIN {{{{ ref('int_ncl_practices') }}}} ncl_practices
    ON src."record_owner_organisation_code" = ncl_practices.practice_code'''
    
    elif pattern == 'infrastructure':
        return f'''{config_block}

/*
Base {table_name} View  
Filters to NCL practices only.
Pattern: Infrastructure table with record_owner_organisation_code
*/

SELECT
{column_list}
FROM {{{{ source('olids_core', '{table_name}') }}}} src
INNER JOIN {{{{ ref('int_ncl_practices') }}}} ncl_practices
    ON src."record_owner_organisation_code" = ncl_practices.practice_code'''
    
    elif pattern == 'generated':
        if table_name == 'PATIENT_PERSON':
            return f'''{config_block}

/*
Base {table_name} View
Generated from filtered patient data.
Pattern: Bridge table generated from patient base
*/

SELECT 
    patients."id" AS patient_id,
    patients."id" AS person_id  -- Generated relationship per issue #192
FROM {{{{ ref('base_olids_patient') }}}} patients'''
        
        elif table_name == 'PERSON':
            return f'''{config_block}

/*
Base {table_name} View
Generated person dimension from filtered patients.
Pattern: Dimension generated from patient base  
*/

SELECT DISTINCT
    "id",
    "nhs_number_hash",
    "title",
    "gender_concept_id",
    "birth_year",
    "birth_month",
    "death_year",
    "death_month"
FROM {{{{ ref('base_olids_patient') }}}}'''
    
    elif pattern == 'reference':
        return f'''{config_block}

/*
Base {table_name} View
Reference data - no filtering applied.
Pattern: Global reference table
*/

SELECT
{column_list}
FROM {{{{ source('olids_core', '{table_name}') }}}} src'''
    
    return None

def generate_all_base_views():
    """Generate all base layer views"""
    # Create base directory if it doesn't exist
    os.makedirs(BASE_DIR, exist_ok=True)
    
    # Load OLIDS tables
    olids_tables = load_sources()
    
    generated_count = 0
    skipped_count = 0
    
    for table in olids_tables:
        table_name = table['name']
        columns = table['columns']
        
        # Determine filtering pattern
        pattern = determine_filtering_pattern(table_name, columns)
        
        # Generate content
        content = generate_base_view_content(table_name, pattern, columns)
        
        if content is None:
            print(f"Skipped {table_name} (manual creation or special case)")
            skipped_count += 1
            continue
            
        # Write file
        filename = f"base_olids_{table_name.lower()}.sql"
        filepath = os.path.join(BASE_DIR, filename)
        
        with open(filepath, 'w') as f:
            f.write(content)
        
        print(f"Generated {filename} (pattern: {pattern})")
        generated_count += 1
    
    print(f"\nBase layer generation complete:")
    print(f"Generated: {generated_count} files")
    print(f"Skipped: {skipped_count} files")
    return generated_count, skipped_count

def update_source_mappings():
    """Add olids_base source to source_mappings.yml"""
    with open(MAPPINGS_FILE, 'r') as f:
        mappings = yaml.safe_load(f)
    
    # Check if olids_base already exists
    existing_sources = [s['source_name'] for s in mappings['sources']]
    if 'olids_base' in existing_sources:
        print("olids_base source already exists in source_mappings.yml")
        return
    
    # Add olids_base source
    base_source = {
        'source_name': 'olids_base',
        'database': 'Data_Store_OLIDS_Alpha',  # Will be overridden by dbt
        'schema': 'OLIDS_FILTERED',  # Virtual schema for base views
        'description': 'Filtered OLIDS base layer - excludes sensitive patients and restricts to NCL practices',
        'staging_prefix': 'stg_olids',
        'domain': 'olids'
    }
    
    mappings['sources'].append(base_source)
    
    # Write back
    with open(MAPPINGS_FILE, 'w') as f:
        yaml.dump(mappings, f, default_flow_style=False, sort_keys=False)
    
    print("Added olids_base source to source_mappings.yml")

if __name__ == "__main__":
    print("Generating OLIDS base layer...")
    
    try:
        # Generate base views
        generated, skipped = generate_all_base_views()
        
        # Update source mappings
        update_source_mappings()
        
        print(f"\nSUCCESS: Base layer generation successful!")
        print(f"Next steps:")
        print(f"1. Review generated files in {BASE_DIR}")
        print(f"2. Run: python scripts/sources/3_generate_staging_models.py --source olids_base")
        print(f"3. Test with: dbt run --models base_olids_patient_filtered int_ncl_practices")
        
    except Exception as e:
        print(f"ERROR: Error generating base layer: {e}")
        sys.exit(1)