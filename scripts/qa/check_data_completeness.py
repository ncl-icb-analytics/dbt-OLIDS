"""
Check data completeness for core fields in source OLIDS tables.
Validates NULL rates for critical fields in the raw source data.
Does not check base views - focuses on underlying data quality.
"""

import os
from dotenv import load_dotenv
from snowflake.snowpark import Session

# Load environment variables
load_dotenv()

# Configuration
SOURCE_DATABASE = '"Data_Store_OLIDS_Alpha"'
WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
USER = os.getenv('SNOWFLAKE_USER')
ROLE = os.getenv('SNOWFLAKE_ROLE')

# Field completeness checks
# Format: (schema, table, field_name)
COMPLETENESS_CHECKS = [
    # Patient table
    ('OLIDS_MASKED', 'PATIENT', 'sk_patient_id'),
    ('OLIDS_MASKED', 'PATIENT', 'birth_year'),
    ('OLIDS_MASKED', 'PATIENT', 'birth_month'),
    ('OLIDS_MASKED', 'PATIENT', 'gender_concept_id'),

    # Episode of care
    ('OLIDS_COMMON', 'EPISODE_OF_CARE', 'id'),
    ('OLIDS_COMMON', 'EPISODE_OF_CARE', 'patient_id'),
    ('OLIDS_COMMON', 'EPISODE_OF_CARE', 'person_id'),
    ('OLIDS_COMMON', 'EPISODE_OF_CARE', 'episode_of_care_start_date'),
    ('OLIDS_COMMON', 'EPISODE_OF_CARE', 'lds_start_date_time'),
    ('OLIDS_COMMON', 'EPISODE_OF_CARE', 'episode_type_source_concept_id'),
    ('OLIDS_COMMON', 'EPISODE_OF_CARE', 'episode_status_source_concept_id'),

    # Observation
    ('OLIDS_COMMON', 'OBSERVATION', 'patient_id'),
    ('OLIDS_COMMON', 'OBSERVATION', 'person_id'),
    ('OLIDS_COMMON', 'OBSERVATION', 'lds_start_date_time'),
    ('OLIDS_COMMON', 'OBSERVATION', 'clinical_effective_date'),
    ('OLIDS_COMMON', 'OBSERVATION', 'observation_source_concept_id'),
    ('OLIDS_COMMON', 'OBSERVATION', 'result_value_units_concept_id'),
    ('OLIDS_COMMON', 'OBSERVATION', 'date_precision_concept_id'),
    ('OLIDS_COMMON', 'OBSERVATION', 'episodicity_concept_id'),

    # Medication Statement
    ('OLIDS_COMMON', 'MEDICATION_STATEMENT', 'patient_id'),
    ('OLIDS_COMMON', 'MEDICATION_STATEMENT', 'person_id'),
    ('OLIDS_COMMON', 'MEDICATION_STATEMENT', 'lds_start_date_time'),
    ('OLIDS_COMMON', 'MEDICATION_STATEMENT', 'clinical_effective_date'),
    ('OLIDS_COMMON', 'MEDICATION_STATEMENT', 'medication_statement_source_concept_id'),
    ('OLIDS_COMMON', 'MEDICATION_STATEMENT', 'authorisation_type_concept_id'),
    ('OLIDS_COMMON', 'MEDICATION_STATEMENT', 'date_precision_concept_id'),

    # Medication Order
    ('OLIDS_COMMON', 'MEDICATION_ORDER', 'patient_id'),
    ('OLIDS_COMMON', 'MEDICATION_ORDER', 'person_id'),
    ('OLIDS_COMMON', 'MEDICATION_ORDER', 'lds_start_date_time'),
    ('OLIDS_COMMON', 'MEDICATION_ORDER', 'clinical_effective_date'),
    ('OLIDS_COMMON', 'MEDICATION_ORDER', 'medication_order_source_concept_id'),
    ('OLIDS_COMMON', 'MEDICATION_ORDER', 'date_precision_concept_id'),

    # Diagnostic Order
    ('OLIDS_COMMON', 'DIAGNOSTIC_ORDER', 'patient_id'),
    ('OLIDS_COMMON', 'DIAGNOSTIC_ORDER', 'person_id'),
    ('OLIDS_COMMON', 'DIAGNOSTIC_ORDER', 'lds_start_date_time'),
    ('OLIDS_COMMON', 'DIAGNOSTIC_ORDER', 'clinical_effective_date'),
    ('OLIDS_COMMON', 'DIAGNOSTIC_ORDER', 'diagnostic_order_source_concept_id'),
    ('OLIDS_COMMON', 'DIAGNOSTIC_ORDER', 'result_value_units_concept_id'),
    ('OLIDS_COMMON', 'DIAGNOSTIC_ORDER', 'date_precision_concept_id'),
    ('OLIDS_COMMON', 'DIAGNOSTIC_ORDER', 'episodicity_concept_id'),

    # Encounter
    ('OLIDS_COMMON', 'ENCOUNTER', 'patient_id'),
    ('OLIDS_COMMON', 'ENCOUNTER', 'person_id'),
    ('OLIDS_COMMON', 'ENCOUNTER', 'lds_start_date_time'),
    ('OLIDS_COMMON', 'ENCOUNTER', 'clinical_effective_date'),
    ('OLIDS_COMMON', 'ENCOUNTER', 'encounter_source_concept_id'),
    ('OLIDS_COMMON', 'ENCOUNTER', 'date_precision_concept_id'),

    # Allergy Intolerance
    ('OLIDS_COMMON', 'ALLERGY_INTOLERANCE', 'patient_id'),
    ('OLIDS_COMMON', 'ALLERGY_INTOLERANCE', 'person_id'),
    ('OLIDS_COMMON', 'ALLERGY_INTOLERANCE', 'lds_start_date_time'),
    ('OLIDS_COMMON', 'ALLERGY_INTOLERANCE', 'clinical_effective_date'),
    ('OLIDS_COMMON', 'ALLERGY_INTOLERANCE', 'allergy_intolerance_source_concept_id'),
    ('OLIDS_COMMON', 'ALLERGY_INTOLERANCE', 'date_precision_concept_id'),

    # Procedure Request
    ('OLIDS_COMMON', 'PROCEDURE_REQUEST', 'lds_start_date_time'),
    ('OLIDS_COMMON', 'PROCEDURE_REQUEST', 'clinical_effective_date'),
    ('OLIDS_COMMON', 'PROCEDURE_REQUEST', 'procedure_request_source_concept_id'),
    ('OLIDS_COMMON', 'PROCEDURE_REQUEST', 'date_precision_concept_id'),
    ('OLIDS_COMMON', 'PROCEDURE_REQUEST', 'status_concept_id'),

    # Referral Request
    ('OLIDS_COMMON', 'REFERRAL_REQUEST', 'lds_start_date_time'),
    ('OLIDS_COMMON', 'REFERRAL_REQUEST', 'clinical_effective_date'),
    ('OLIDS_COMMON', 'REFERRAL_REQUEST', 'referral_request_source_concept_id'),
    ('OLIDS_COMMON', 'REFERRAL_REQUEST', 'date_precision_concept_id'),
    ('OLIDS_COMMON', 'REFERRAL_REQUEST', 'referral_request_priority_concept_id'),
    ('OLIDS_COMMON', 'REFERRAL_REQUEST', 'referral_request_type_concept_id'),
    ('OLIDS_COMMON', 'REFERRAL_REQUEST', 'referral_request_specialty_concept_id'),

    # Location Contact
    ('OLIDS_COMMON', 'LOCATION_CONTACT', 'contact_type_concept_id'),

    # Appointment
    ('OLIDS_COMMON', 'APPOINTMENT', 'appointment_status_concept_id'),
    ('OLIDS_COMMON', 'APPOINTMENT', 'booking_method_concept_id'),
    ('OLIDS_COMMON', 'APPOINTMENT', 'contact_mode_concept_id'),

    # Flag
    ('OLIDS_COMMON', 'FLAG', 'lds_start_date_time'),

    # Patient Address
    ('OLIDS_MASKED', 'PATIENT_ADDRESS', 'address_type_concept_id'),

    # Patient Contact
    ('OLIDS_MASKED', 'PATIENT_CONTACT', 'contact_type_concept_id'),
]


def check_field_completeness(session, schema, table, field):
    """Check completeness for a single field."""
    query = f"""
    SELECT
        '{table}' AS table_name,
        '{field}' AS field_name,
        COUNT(*) AS total_records,
        COALESCE(SUM(CASE WHEN {field} IS NULL THEN 1 ELSE 0 END), 0) AS null_count,
        ROUND(100.0 * COALESCE(SUM(CASE WHEN {field} IS NULL THEN 1 ELSE 0 END), 0) / NULLIF(COUNT(*), 0), 4) AS null_percentage
    FROM {SOURCE_DATABASE}.{schema}.{table}
    """

    result = session.sql(query).collect()
    return result[0] if result else None


def main():
    """Execute data completeness checks."""
    print(f"\n{'='*100}")
    print(f"DATA COMPLETENESS CHECK")
    print(f"{'='*100}")
    print(f"Database: {SOURCE_DATABASE}")

    print(f"\nConnecting to Snowflake...")

    # Create Snowpark session with SSO authentication
    connection_parameters = {
        "account": ACCOUNT,
        "user": USER,
        "authenticator": "externalbrowser",
        "warehouse": WAREHOUSE,
        "role": ROLE
    }

    session = Session.builder.configs(connection_parameters).create()
    print("✓ Connected successfully")

    try:
        # ========================================
        # FIELD COMPLETENESS CHECKS
        # ========================================
        print(f"\n{'='*100}")
        print("FIELD COMPLETENESS CHECKS")
        print(f"{'='*100}")
        print(f"\nChecking {len(COMPLETENESS_CHECKS)} fields...")

        results = []
        for i, (schema, table, field) in enumerate(COMPLETENESS_CHECKS, 1):
            print(f"  Checking {i}/{len(COMPLETENESS_CHECKS)}: {table}.{field}")
            result = check_field_completeness(session, schema, table, field)
            if result:
                results.append({
                    'table': result['TABLE_NAME'],
                    'field': result['FIELD_NAME'],
                    'total_records': int(result['TOTAL_RECORDS']),
                    'null_count': int(result['NULL_COUNT']),
                    'null_percentage': float(result['NULL_PERCENTAGE']) if result['NULL_PERCENTAGE'] else 0.0
                })

        print("✓ Completeness checks completed\n")

        # Display results
        print("="*100)
        print("RESULTS")
        print("="*100)

        # Show fields with NULL values
        incomplete = [r for r in results if r['null_count'] > 0]
        if incomplete:
            print(f"\n⚠️  INCOMPLETE FIELDS ({len(incomplete)} fields with NULL values):\n")
            for r in incomplete:
                print(f"  {r['table']}.{r['field']}")
                print(f"    Total records: {r['total_records']:,}")
                print(f"    NULL count: {r['null_count']:,} ({r['null_percentage']:.4f}%)")
                print()
        else:
            print("\n✓ All fields are complete (no NULL values)!")

        # Show complete fields summary
        complete = [r for r in results if r['null_count'] == 0]
        if complete:
            print(f"\n✓ COMPLETE FIELDS ({len(complete)} fields):\n")
            for r in complete:
                print(f"  {r['table']}.{r['field']}")
                print(f"    Total records: {r['total_records']:,}")

        # ========================================
        # SUMMARY STATISTICS
        # ========================================
        print(f"\n\n{'='*100}")
        print("SUMMARY")
        print(f"{'='*100}")

        total_fields = len(results)
        incomplete_fields = len(incomplete)
        complete_fields = len(complete)

        print(f"\nTotal fields checked: {total_fields}")
        print(f"Fields with NULL values: {incomplete_fields}")
        print(f"Fields complete: {complete_fields}")

    finally:
        session.close()
        print("\nConnection closed")


if __name__ == '__main__':
    main()
