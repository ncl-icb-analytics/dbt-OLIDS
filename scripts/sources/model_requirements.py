"""
Model requirements configuration for base layer generation.
Defines which fields must not be NULL for data quality and incremental processing.
"""

# Model-specific requirements
# Each model can have:
# - required_fields: Fields that must not be NULL (filtered out if NULL)
# - incremental_key: Field to use for incremental processing
# - clustering_keys: Fields to use for clustering in stable layer

MODEL_REQUIREMENTS = {
    'OBSERVATION': {
        'required_fields': [
            'observation_source_concept_id',  # Must know what the observation is
            'clinical_effective_date',        # Must know when it occurred
            'lds_start_date_time'             # Required for incremental processing
        ],
        'incremental_key': 'lds_start_date_time',
        'clustering_keys': ['observation_source_concept_id', 'clinical_effective_date'],
        'description': 'Clinical observations - requires concept ID and date'
    },

    'MEDICATION_ORDER': {
        'required_fields': [
            'medication_order_source_concept_id',  # Must know what medication
            'clinical_effective_date',             # Must know when prescribed
            'lds_start_date_time'                  # Required for incremental
        ],
        'incremental_key': 'lds_start_date_time',
        'clustering_keys': ['medication_order_source_concept_id', 'clinical_effective_date'],
        'description': 'Medication orders - requires concept ID and date'
    },

    'MEDICATION_STATEMENT': {
        'required_fields': [
            'medication_statement_source_concept_id',  # Must know what medication
            'clinical_effective_date',                 # Must know when
            'lds_start_date_time'                      # Required for incremental
        ],
        'incremental_key': 'lds_start_date_time',
        'clustering_keys': ['medication_statement_source_concept_id', 'clinical_effective_date'],
        'description': 'Medication statements - requires concept ID and date'
    },

    'PATIENT': {
        'required_fields': [
            'id',                      # Primary key
            'lds_start_date_time'      # Required for incremental
        ],
        'incremental_key': 'lds_start_date_time',
        'clustering_keys': ['id'],
        'description': 'Patient demographics'
    },

    'PATIENT_PERSON': {
        'required_fields': [
            'patient_id',              # Mapping key
            'person_id',               # Mapping key
            'lds_start_date_time'      # Required for incremental
        ],
        'incremental_key': 'lds_start_date_time',
        'clustering_keys': ['person_id', 'patient_id'],
        'description': 'Patient to person mapping'
    },

    'PERSON': {
        'required_fields': [
            'id'                       # Primary key (generated from patient)
        ],
        'incremental_key': None,       # Derived from patient, not incremental
        'clustering_keys': ['id'],
        'description': 'Person dimension derived from patients'
    },

    'ORGANISATION': {
        'required_fields': [
            'organisation_code',       # Primary identifier
            'lds_start_date_time'      # Required for incremental
        ],
        'incremental_key': 'lds_start_date_time',
        'clustering_keys': ['organisation_code'],
        'description': 'Organisation reference data'
    },

    'APPOINTMENT': {
        'required_fields': [
            'patient_id',              # Must have patient
            'start_date',              # Must have appointment date
            'lds_start_date_time'      # Required for incremental
        ],
        'incremental_key': 'lds_start_date_time',
        'clustering_keys': ['patient_id', 'start_date'],
        'description': 'Patient appointments'
    },

    'PATIENT_ADDRESS': {
        'required_fields': [
            'patient_id',              # Must have patient
            'start_date',              # SCD2 validity date
            'lds_start_date_time'      # Required for incremental
        ],
        'incremental_key': 'lds_start_date_time',
        'clustering_keys': ['patient_id', 'start_date'],
        'description': 'Patient address history (SCD2)'
    },

    'PATIENT_REGISTERED_PRACTITIONER_IN_ROLE': {
        'required_fields': [
            'patient_id',              # Must have patient
            'lds_start_date_time'      # Required for incremental
        ],
        'incremental_key': 'lds_start_date_time',
        'clustering_keys': ['patient_id'],
        'description': 'Patient GP registration'
    },

    'TERMINOLOGY_CONCEPT': {
        'required_fields': [
            'id',                      # Primary key
            'lds_start_date_time'      # Required for incremental
        ],
        'incremental_key': 'lds_start_date_time',
        'clustering_keys': ['id'],
        'description': 'Terminology concepts reference'
    },

    'TERMINOLOGY_CONCEPT_MAP': {
        'required_fields': [
            'source_code_id',          # Mapping key
            'target_code_id',          # Mapping key
            'lds_start_date_time'      # Required for incremental
        ],
        'incremental_key': 'lds_start_date_time',
        'clustering_keys': ['source_code_id', 'target_code_id'],
        'description': 'Concept mapping reference'
    },

    'EPISODE_OF_CARE': {
        'required_fields': [
            'patient_id',              # Must have patient
            'lds_start_date_time'      # Required for incremental
        ],
        'incremental_key': 'lds_start_date_time',
        'clustering_keys': ['patient_id'],
        'description': 'Episode of care records'
    },

    # Tables with no special requirements (reference data or unused)
    'ALLERGY_INTOLERANCE': {
        'required_fields': ['lds_start_date_time'],
        'incremental_key': 'lds_start_date_time',
        'clustering_keys': ['patient_id'],
        'description': 'Allergy and intolerance records'
    },

    'DIAGNOSTIC_ORDER': {
        'required_fields': ['lds_start_date_time'],
        'incremental_key': 'lds_start_date_time',
        'clustering_keys': ['patient_id'],
        'description': 'Diagnostic orders'
    },

    'ENCOUNTER': {
        'required_fields': ['lds_start_date_time'],
        'incremental_key': 'lds_start_date_time',
        'clustering_keys': ['patient_id'],
        'description': 'Clinical encounters'
    },

    'FLAG': {
        'required_fields': ['lds_start_date_time'],
        'incremental_key': 'lds_start_date_time',
        'clustering_keys': ['patient_id'],
        'description': 'Clinical flags and alerts'
    },

    'PROCEDURE_REQUEST': {
        'required_fields': ['lds_start_date_time'],
        'incremental_key': 'lds_start_date_time',
        'clustering_keys': ['patient_id'],
        'description': 'Procedure requests'
    },

    'REFERRAL_REQUEST': {
        'required_fields': ['lds_start_date_time'],
        'incremental_key': 'lds_start_date_time',
        'clustering_keys': ['patient_id'],
        'description': 'Referral requests'
    }
}

def get_model_requirements(table_name):
    """Get requirements for a specific model"""
    return MODEL_REQUIREMENTS.get(table_name, {
        'required_fields': [],
        'incremental_key': 'lds_start_date_time',
        'clustering_keys': [],
        'description': f'{table_name} table'
    })

def generate_where_clause(table_name):
    """Generate WHERE clause for filtering NULLs in required fields"""
    requirements = get_model_requirements(table_name)
    required_fields = requirements.get('required_fields', [])

    if not required_fields:
        return ""

    # Build WHERE conditions for each required field
    conditions = []
    for field in required_fields:
        # Check if field exists in source (src.) or joined table (pp.)
        if field == 'person_id' and table_name in ['OBSERVATION', 'MEDICATION_ORDER', 'MEDICATION_STATEMENT']:
            # person_id comes from patient_person join
            conditions.append(f"pp.{field} IS NOT NULL")
        else:
            conditions.append(f'src."{field}" IS NOT NULL')

    if conditions:
        return "WHERE " + "\n    AND ".join(conditions)
    return ""

def get_incremental_key(table_name):
    """Get the incremental processing key for a table"""
    requirements = get_model_requirements(table_name)
    return requirements.get('incremental_key', 'lds_start_date_time')

def get_clustering_keys(table_name):
    """Get the clustering keys for a table"""
    requirements = get_model_requirements(table_name)
    return requirements.get('clustering_keys', [])