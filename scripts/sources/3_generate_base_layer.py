import yaml
import os
import sys
from model_requirements import get_model_requirements, generate_where_clause

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

    # Find olids_core and olids_terminology sources
    all_tables = []
    for source in sources_data['sources']:
        if source['name'] in ['olids_core', 'olids_terminology']:
            for table in source['tables']:
                table_info = {
                    'name': table['name'],
                    'source_name': source['name'],
                    'columns': [col['name'] for col in table.get('columns', [])]
                }
                all_tables.append(table_info)

    return all_tables

def determine_filtering_pattern(table_name, columns, source_name):
    """Determine which filtering pattern to apply based on table structure and source"""
    has_patient_id = 'patient_id' in columns
    has_record_owner = 'record_owner_organisation_code' in columns

    # Handle olids_terminology tables
    if source_name == 'olids_terminology':
        return 'terminology'  # Unquoted identifiers only

    # Special cases for olids_core
    if table_name == 'PATIENT':
        return 'patient_base'  # Generate base_olids_patient with filtering logic
    elif table_name in ['PATIENT_PERSON', 'PERSON']:
        return 'generated'  # Special handling for generated tables
    elif table_name in ['LOCATION', 'LOCATION_CONTACT', 'ORGANISATION',
                        'PATIENT_UPRN']:
        return 'reference'  # No filtering needed
    elif has_patient_id and has_record_owner:
        return 'clinical'  # Patient + organisation filtering
    elif has_record_owner:
        return 'infrastructure'  # Organisation filtering only
    else:
        return 'reference'  # No filtering needed

def generate_column_list(columns, has_person_id_replacement=False):
    """Generate explicit column list for SELECT statement

    Args:
        columns: List of column names
        has_person_id_replacement: Whether to replace person_id with fabricated version
    """
    if not columns:
        return "src.*"

    # Format columns - select quoted source columns as unquoted target columns
    formatted_columns = []
    for col in columns:
        if col == 'person_id' and has_person_id_replacement:
            # Get person_id from patient_person mapping
            formatted_columns.append(f'    pp.person_id AS {col}')
        else:
            formatted_columns.append(f'    src."{col}" AS {col}')

    return ",\n".join(formatted_columns)

def generate_base_view_content(table_name, pattern, columns=None, source_name='olids_core'):
    """Generate the SQL content for a base view based on the filtering pattern"""

    # Use table name as alias (e.g., PATIENT, OBSERVATION, etc.)
    config_block = f'''{{{{
    config(
        secure=true,
        alias='{table_name.lower()}')
}}}}'''

    if pattern == 'patient_base':
        # Generate column list
        column_list = generate_column_list(columns)

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
        # Check if this table has person_id that needs replacement
        has_person_id = 'person_id' in columns if columns else False
        has_patient_id = 'patient_id' in columns if columns else False
        needs_person_id_fix = has_person_id and has_patient_id

        # Generate column list with person_id replacement if needed
        column_list = generate_column_list(columns, has_person_id_replacement=needs_person_id_fix)

        # Build the FROM clause and JOINs
        join_clause = f'''FROM {{{{ source('olids_core', '{table_name}') }}}} src
INNER JOIN {{{{ ref('base_olids_patient') }}}} patients
    ON src."patient_id" = patients.id'''

        # Add patient_person join if we need to fix person_id
        if needs_person_id_fix:
            join_clause += f'''
INNER JOIN {{{{ ref('base_olids_patient_person') }}}} pp
    ON src."patient_id" = pp.patient_id'''

        join_clause += f'''
INNER JOIN {{{{ ref('int_ncl_practices') }}}} ncl_practices
    ON src."record_owner_organisation_code" = ncl_practices.practice_code'''

        comment = f'''/*
Base {table_name} View
Filters to NCL practices and excludes sensitive patients.
Pattern: Clinical table with patient_id + record_owner_organisation_code'''

        if needs_person_id_fix:
            comment += '''
Note: person_id replaced with fabricated version from patient_person mapping'''

        comment += '''
*/'''

        # Add WHERE clause for required fields
        where_clause = generate_where_clause(table_name)

        return f'''{config_block}

{comment}

SELECT
{column_list}
{join_clause}
{where_clause}'''
    
    elif pattern == 'infrastructure':
        # Generate column list
        column_list = generate_column_list(columns)

        # Add WHERE clause for required fields
        where_clause = generate_where_clause(table_name)

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
    ON src."record_owner_organisation_code" = ncl_practices.practice_code
{where_clause}'''
    
    elif pattern == 'generated':
        if table_name == 'PATIENT_PERSON':
            return f'''{config_block}

/*
Base {table_name} View
Generated from filtered patient data with deterministic person_id.
Pattern: Bridge table generated from patient base
*/

SELECT
    -- New Alpha columns first (following PATIENT_PERSON structure order)
    lds_lakehouse_date_processed AS lakehousedateprocessed,
    lds_lakehouse_datetime_updated AS lakehousedatetimeupdated,
    lds_record_id,
    -- Generate deterministic lds_id for patient_person bridge
    'pp-' || MD5(sk_patient_id) AS lds_id,
    -- Generate deterministic id for this bridge record
    'pp-' || MD5(sk_patient_id) AS id,
    lds_datetime_data_acquired,
    lds_start_date_time,
    lds_dataset_id,
    id AS patient_id,
    -- Generate deterministic person_id from sk_patient_id
    'ncl-person-' || MD5(sk_patient_id) AS person_id
FROM {{{{ ref('base_olids_patient') }}}} patients
WHERE sk_patient_id IS NOT NULL
    AND LENGTH(TRIM(sk_patient_id)) > 0  -- Ensure sk_patient_id is not empty after trimming
    AND is_dummy_patient = FALSE'''
        
        elif table_name == 'PERSON':
            return f'''{config_block}

/*
Base {table_name} View
Generated person dimension from filtered patients.
Pattern: Dimension generated from patient base  
*/

SELECT DISTINCT
    id,
    nhs_number_hash,
    title,
    gender_concept_id,
    birth_year,
    birth_month,
    death_year,
    death_month
FROM {{{{ ref('base_olids_patient') }}}}'''
    
    elif pattern == 'reference':
        # Generate column list
        column_list = generate_column_list(columns)

        # Add WHERE clause for required fields if any
        where_clause = generate_where_clause(table_name)

        return f'''{config_block}

/*
Base {table_name} View
Reference data - no filtering applied.
Pattern: Global reference table
*/

SELECT
{column_list}
FROM {{{{ source('olids_core', '{table_name}') }}}} src
{where_clause}'''

    elif pattern == 'terminology':
        # Generate column list with unquoted identifiers
        column_list = generate_column_list(columns)

        # Add WHERE clause for required fields if any
        where_clause = generate_where_clause(table_name)

        return f'''{config_block}

/*
Base {table_name} View
Terminology data with unquoted identifiers.
Pattern: Terminology table from OLIDS_TERMINOLOGY schema
*/

SELECT
{column_list}
FROM {{{{ source('olids_terminology', '{table_name}') }}}} src
{where_clause}'''

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
        source_name = table['source_name']
        columns = table['columns']

        # Determine filtering pattern
        pattern = determine_filtering_pattern(table_name, columns, source_name)

        # Generate content
        content = generate_base_view_content(table_name, pattern, columns, source_name)
        
        if content is None:
            print(f"Skipped {table_name} (manual creation or special case)")
            skipped_count += 1
            continue
            
        # Write file with appropriate naming pattern
        if source_name == 'olids_terminology':
            filename = f"base_olids_terminology_{table_name.lower()}.sql"
        else:
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