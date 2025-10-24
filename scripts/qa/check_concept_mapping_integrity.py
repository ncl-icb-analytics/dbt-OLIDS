"""
Check concept mapping integrity across all OLIDS tables.
Tests both source concept mappings (via CONCEPT_MAP) and direct concept lookups.
Reports failure rates, counts, and distinct concept IDs.
"""

import os
from dotenv import load_dotenv
from snowflake.snowpark import Session

# Load environment variables
load_dotenv()

# Configuration
SOURCE_DATABASE = '"Data_Store_OLIDS_Alpha"'  # Database containing OLIDS_COMMON, OLIDS_MASKED, and OLIDS_TERMINOLOGY schemas
WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
USER = os.getenv('SNOWFLAKE_USER')
ROLE = os.getenv('SNOWFLAKE_ROLE')

# Define tables, schemas, and their concept fields
# All concept fields use the same pattern: concept_id → CONCEPT_MAP.source_code_id → CONCEPT_MAP.target_code_id → CONCEPT.id
# Format: (schema, table_name, concept_field)
CONCEPT_CHECKS = [
    # OLIDS_COMMON tables
    ('OLIDS_COMMON', 'OBSERVATION', 'observation_source_concept_id'),
    ('OLIDS_COMMON', 'OBSERVATION', 'result_value_units_concept_id'),
    ('OLIDS_COMMON', 'OBSERVATION', 'date_precision_concept_id'),
    ('OLIDS_COMMON', 'OBSERVATION', 'episodicity_concept_id'),
    ('OLIDS_COMMON', 'MEDICATION_STATEMENT', 'medication_statement_source_concept_id'),
    ('OLIDS_COMMON', 'MEDICATION_STATEMENT', 'authorisation_type_concept_id'),
    ('OLIDS_COMMON', 'MEDICATION_STATEMENT', 'date_precision_concept_id'),
    ('OLIDS_COMMON', 'MEDICATION_ORDER', 'medication_order_source_concept_id'),
    ('OLIDS_COMMON', 'MEDICATION_ORDER', 'date_precision_concept_id'),
    ('OLIDS_COMMON', 'DIAGNOSTIC_ORDER', 'diagnostic_order_source_concept_id'),
    ('OLIDS_COMMON', 'DIAGNOSTIC_ORDER', 'result_value_units_concept_id'),
    ('OLIDS_COMMON', 'DIAGNOSTIC_ORDER', 'date_precision_concept_id'),
    ('OLIDS_COMMON', 'DIAGNOSTIC_ORDER', 'episodicity_concept_id'),
    ('OLIDS_COMMON', 'PROCEDURE_REQUEST', 'procedure_request_source_concept_id'),
    ('OLIDS_COMMON', 'PROCEDURE_REQUEST', 'date_precision_concept_id'),
    ('OLIDS_COMMON', 'PROCEDURE_REQUEST', 'status_concept_id'),
    ('OLIDS_COMMON', 'REFERRAL_REQUEST', 'referral_request_source_concept_id'),
    ('OLIDS_COMMON', 'REFERRAL_REQUEST', 'date_precision_concept_id'),
    ('OLIDS_COMMON', 'REFERRAL_REQUEST', 'referral_request_priority_concept_id'),
    ('OLIDS_COMMON', 'REFERRAL_REQUEST', 'referral_request_type_concept_id'),
    ('OLIDS_COMMON', 'REFERRAL_REQUEST', 'referral_request_specialty_concept_id'),
    ('OLIDS_COMMON', 'ALLERGY_INTOLERANCE', 'allergy_intolerance_source_concept_id'),
    ('OLIDS_COMMON', 'ALLERGY_INTOLERANCE', 'date_precision_concept_id'),
    ('OLIDS_COMMON', 'ENCOUNTER', 'encounter_source_concept_id'),
    ('OLIDS_COMMON', 'ENCOUNTER', 'date_precision_concept_id'),
    ('OLIDS_COMMON', 'EPISODE_OF_CARE', 'episode_type_source_concept_id'),
    ('OLIDS_COMMON', 'EPISODE_OF_CARE', 'episode_status_source_concept_id'),
    ('OLIDS_COMMON', 'LOCATION_CONTACT', 'contact_type_concept_id'),
    ('OLIDS_COMMON', 'APPOINTMENT', 'appointment_status_concept_id'),
    ('OLIDS_COMMON', 'APPOINTMENT', 'booking_method_concept_id'),
    ('OLIDS_COMMON', 'APPOINTMENT', 'contact_mode_concept_id'),

    # OLIDS_MASKED tables
    ('OLIDS_MASKED', 'PATIENT', 'gender_concept_id'),
    ('OLIDS_MASKED', 'PATIENT_ADDRESS', 'address_type_concept_id'),
    ('OLIDS_MASKED', 'PATIENT_CONTACT', 'contact_type_concept_id'),
]


def generate_concept_check_query(schema_name, table_name, concept_field):
    """Generate SQL for concept mapping check (concept_id → CONCEPT_MAP → CONCEPT)."""
    return f"""
    SELECT
        '{table_name}' AS table_name,
        '{concept_field}' AS concept_field,
        COUNT(DISTINCT base.{concept_field}) AS total_distinct_concepts,
        SUM(CASE WHEN base.{concept_field} IS NOT NULL THEN 1 ELSE 0 END) AS total_rows_with_concept,
        COUNT(DISTINCT CASE WHEN cm.source_code_id IS NULL THEN base.{concept_field} END) AS failed_concept_map_lookup,
        SUM(CASE WHEN cm.source_code_id IS NULL THEN 1 ELSE 0 END) AS affected_rows_concept_map,
        COUNT(DISTINCT CASE WHEN cm.source_code_id IS NOT NULL AND c.id IS NULL THEN base.{concept_field} END) AS failed_target_concept_lookup,
        SUM(CASE WHEN cm.source_code_id IS NOT NULL AND c.id IS NULL THEN 1 ELSE 0 END) AS affected_rows_target_concept,
        COUNT(DISTINCT CASE WHEN cm.source_code_id IS NOT NULL AND c.id IS NOT NULL AND c.code IS NULL THEN base.{concept_field} END) AS null_code,
        SUM(CASE WHEN cm.source_code_id IS NOT NULL AND c.id IS NOT NULL AND c.code IS NULL THEN 1 ELSE 0 END) AS affected_rows_null_code,
        COUNT(DISTINCT CASE WHEN cm.source_code_id IS NOT NULL AND c.id IS NOT NULL AND c.display IS NULL THEN base.{concept_field} END) AS null_display,
        SUM(CASE WHEN cm.source_code_id IS NOT NULL AND c.id IS NOT NULL AND c.display IS NULL THEN 1 ELSE 0 END) AS affected_rows_null_display
    FROM {SOURCE_DATABASE}.{schema_name}.{table_name} base
    LEFT JOIN {SOURCE_DATABASE}.OLIDS_TERMINOLOGY.CONCEPT_MAP cm
        ON base.{concept_field} = cm.source_code_id
    LEFT JOIN {SOURCE_DATABASE}.OLIDS_TERMINOLOGY.CONCEPT c
        ON cm.target_code_id = c.id
    WHERE base.{concept_field} IS NOT NULL
    """


def build_full_query():
    """Build complete UNION ALL query for all concept checks."""
    queries = []

    for schema_name, table_name, concept_field in CONCEPT_CHECKS:
        queries.append(generate_concept_check_query(schema_name, table_name, concept_field))

    union_query = "\nUNION ALL\n".join(queries)

    return f"""
    WITH all_checks AS (
        {union_query}
    )
    SELECT
        table_name,
        concept_field,
        total_distinct_concepts,
        total_rows_with_concept,
        failed_concept_map_lookup,
        affected_rows_concept_map,
        failed_target_concept_lookup,
        affected_rows_target_concept,
        null_code,
        affected_rows_null_code,
        null_display,
        affected_rows_null_display,
        (failed_concept_map_lookup + failed_target_concept_lookup + null_code) AS total_failures,
        (affected_rows_concept_map + affected_rows_target_concept + affected_rows_null_code) AS total_affected_rows,
        ROUND(100.0 * (failed_concept_map_lookup + failed_target_concept_lookup + null_code) / NULLIF(total_distinct_concepts, 0), 2) AS failure_percentage
    FROM all_checks
    ORDER BY
        CASE WHEN (failed_concept_map_lookup + failed_target_concept_lookup + null_code) > 0 THEN 0 ELSE 1 END,
        failure_percentage DESC,
        null_display DESC,
        table_name,
        concept_field
    """


def main():
    """Execute concept mapping integrity checks."""
    print(f"\n{'='*100}")
    print(f"CONCEPT MAPPING INTEGRITY CHECK")
    print(f"{'='*100}")
    print(f"Database: {SOURCE_DATABASE}")
    print(f"Total checks configured: {len(CONCEPT_CHECKS)}")

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
        print("\nBuilding SQL query...")
        query = build_full_query()
        print("✓ Query built")

        print("\nExecuting integrity checks (this may take a few minutes)...")
        df = session.sql(query).to_pandas()
        print("✓ Checks completed\n")

        # Display results
        print("="*100)
        print("RESULTS")
        print("="*100)

        # Show failed mappings (including NULL codes as hard failures)
        failed = df[df['TOTAL_FAILURES'] > 0]
        if not failed.empty:
            print(f"\n⚠️  FAILED MAPPINGS ({len(failed)} fields):\n")
            for _, row in failed.iterrows():
                print(f"  {row['TABLE_NAME']}.{row['CONCEPT_FIELD']}")
                print(f"    Total distinct concepts: {int(row['TOTAL_DISTINCT_CONCEPTS']):,}")
                print(f"    Total rows with concept: {int(row['TOTAL_ROWS_WITH_CONCEPT']):,}")
                print(f"    Failed concepts: {int(row['TOTAL_FAILURES']):,} ({int(row['FAILURE_PERCENTAGE'])}%)")
                print(f"    Affected rows: {int(row['TOTAL_AFFECTED_ROWS']):,}")
                if row['FAILED_CONCEPT_MAP_LOOKUP'] > 0:
                    print(f"    ↳ Missing in CONCEPT_MAP: {int(row['FAILED_CONCEPT_MAP_LOOKUP']):,} concepts ({int(row['AFFECTED_ROWS_CONCEPT_MAP']):,} rows)")
                if row['FAILED_TARGET_CONCEPT_LOOKUP'] > 0:
                    print(f"    ↳ Missing target in CONCEPT: {int(row['FAILED_TARGET_CONCEPT_LOOKUP']):,} concepts ({int(row['AFFECTED_ROWS_TARGET_CONCEPT']):,} rows)")
                if row['NULL_CODE'] > 0:
                    print(f"    ↳ NULL code in CONCEPT: {int(row['NULL_CODE']):,} concepts ({int(row['AFFECTED_ROWS_NULL_CODE']):,} rows) (hard fail)")
                if row['NULL_DISPLAY'] > 0:
                    print(f"    ⚠️  NULL display: {int(row['NULL_DISPLAY']):,} concepts ({int(row['AFFECTED_ROWS_NULL_DISPLAY']):,} rows)")
                print()
        else:
            print("\n✓ No failed mappings detected!")

        # Show warnings for fields with null displays but no hard failures
        warnings = df[(df['TOTAL_FAILURES'] == 0) & (df['NULL_DISPLAY'] > 0)]
        if not warnings.empty:
            print(f"\n⚠️  DATA QUALITY WARNINGS ({len(warnings)} fields with null displays):\n")
            for _, row in warnings.iterrows():
                print(f"  {row['TABLE_NAME']}.{row['CONCEPT_FIELD']}")
                print(f"    Total distinct concepts: {int(row['TOTAL_DISTINCT_CONCEPTS']):,}")
                print(f"    Total rows with concept: {int(row['TOTAL_ROWS_WITH_CONCEPT']):,}")
                print(f"    ⚠️  NULL display: {int(row['NULL_DISPLAY']):,} concepts ({int(row['AFFECTED_ROWS_NULL_DISPLAY']):,} rows)")
                print()

        # Show successful mappings summary (no failures and no warnings)
        successful = df[(df['TOTAL_FAILURES'] == 0) & (df['NULL_DISPLAY'] == 0)]
        if not successful.empty:
            print(f"\n✓ SUCCESSFUL MAPPINGS ({len(successful)} fields - no failures or warnings):\n")
            for _, row in successful.iterrows():
                # Skip if no distinct concepts (table has no data for this field)
                if row['TOTAL_DISTINCT_CONCEPTS'] == 0:
                    continue
                print(f"  {row['TABLE_NAME']}.{row['CONCEPT_FIELD']}")
                print(f"    Distinct concepts: {int(row['TOTAL_DISTINCT_CONCEPTS']):,}")
                print(f"    Total rows: {int(row['TOTAL_ROWS_WITH_CONCEPT']):,}")

        # Summary statistics
        print("\n" + "="*100)
        print("SUMMARY")
        print("="*100)
        total_concepts = int(df['TOTAL_DISTINCT_CONCEPTS'].sum())
        total_rows = int(df['TOTAL_ROWS_WITH_CONCEPT'].sum())
        total_failures = int(df['TOTAL_FAILURES'].sum())
        total_affected_rows = int(df['TOTAL_AFFECTED_ROWS'].sum())
        total_null_display = int(df['NULL_DISPLAY'].sum())
        overall_failure_rate = (total_failures / total_concepts * 100) if total_concepts > 0 else 0

        print(f"Total distinct concepts checked: {total_concepts:,}")
        print(f"Total rows checked: {total_rows:,}")
        print(f"Total hard failures: {total_failures:,} ({overall_failure_rate:.4f}%)")
        print(f"Total affected rows: {total_affected_rows:,}")
        print(f"Total NULL displays (warning): {total_null_display:,}")
        print(f"\nFields with hard failures: {len(failed)}/{len(df)}")
        print(f"Fields with warnings only: {len(warnings)}/{len(df)}")
        print(f"Fields fully successful: {len(successful)}/{len(df)}")

    finally:
        session.close()
        print("\nConnection closed")


if __name__ == '__main__':
    main()
