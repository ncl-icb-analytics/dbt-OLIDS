"""
Export detailed concept mapping failures for investigation.
Exports CSV files containing failed mappings grouped by failure type.
Complements check_concept_mapping_integrity.py by providing actionable lists for remediation.
"""

import os
from datetime import datetime
from pathlib import Path
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

# Output directory
OUTPUT_DIR = Path(__file__).parent / 'output'
OUTPUT_DIR.mkdir(exist_ok=True)

# Reuse same configuration as integrity check
CONCEPT_CHECKS = [
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
    ('OLIDS_MASKED', 'PATIENT', 'gender_concept_id'),
    ('OLIDS_MASKED', 'PATIENT_ADDRESS', 'address_type_concept_id'),
    ('OLIDS_MASKED', 'PATIENT_CONTACT', 'contact_type_concept_id'),
]


def build_missing_concept_map_query():
    """Build query to extract concepts missing from CONCEPT_MAP. Only returns concept_id and row_count."""
    queries = []

    for schema_name, table_name, concept_field in CONCEPT_CHECKS:
        queries.append(f"""
        SELECT
            '{table_name}' AS table_name,
            '{concept_field}' AS concept_field,
            base.{concept_field} AS concept_id,
            COUNT(*) AS row_count
        FROM {SOURCE_DATABASE}.{schema_name}.{table_name} base
        LEFT JOIN {SOURCE_DATABASE}.OLIDS_TERMINOLOGY.CONCEPT_MAP cm
            ON base.{concept_field} = cm.source_code_id
        WHERE base.{concept_field} IS NOT NULL
            AND cm.source_code_id IS NULL
        GROUP BY base.{concept_field}
        """)

    return "\nUNION ALL\n".join(queries)


def build_null_display_query():
    """Build query to extract concepts with NULL display."""
    queries = []

    for schema_name, table_name, concept_field in CONCEPT_CHECKS:
        queries.append(f"""
        SELECT
            '{table_name}' AS table_name,
            '{concept_field}' AS concept_field,
            base.{concept_field} AS concept_id,
            c.code,
            c.system,
            COUNT(*) AS row_count
        FROM {SOURCE_DATABASE}.{schema_name}.{table_name} base
        INNER JOIN {SOURCE_DATABASE}.OLIDS_TERMINOLOGY.CONCEPT_MAP cm
            ON base.{concept_field} = cm.source_code_id
        INNER JOIN {SOURCE_DATABASE}.OLIDS_TERMINOLOGY.CONCEPT c
            ON cm.target_code_id = c.id
        WHERE base.{concept_field} IS NOT NULL
            AND c.display IS NULL
        GROUP BY base.{concept_field}, c.code, c.system
        """)

    return "\nUNION ALL\n".join(queries)


def build_missing_target_concept_query():
    """Build query to extract concepts with CONCEPT_MAP entry but missing target. Only returns concept_id and row_count."""
    queries = []

    for schema_name, table_name, concept_field in CONCEPT_CHECKS:
        queries.append(f"""
        SELECT
            '{table_name}' AS table_name,
            '{concept_field}' AS concept_field,
            base.{concept_field} AS concept_id,
            COUNT(*) AS row_count
        FROM {SOURCE_DATABASE}.{schema_name}.{table_name} base
        INNER JOIN {SOURCE_DATABASE}.OLIDS_TERMINOLOGY.CONCEPT_MAP cm
            ON base.{concept_field} = cm.source_code_id
        LEFT JOIN {SOURCE_DATABASE}.OLIDS_TERMINOLOGY.CONCEPT c
            ON cm.target_code_id = c.id
        WHERE base.{concept_field} IS NOT NULL
            AND c.id IS NULL
        GROUP BY base.{concept_field}
        """)

    return "\nUNION ALL\n".join(queries)


def build_null_code_query():
    """Build query to extract concepts with NULL code. Returns concept_id, system, and row_count."""
    queries = []

    for schema_name, table_name, concept_field in CONCEPT_CHECKS:
        queries.append(f"""
        SELECT
            '{table_name}' AS table_name,
            '{concept_field}' AS concept_field,
            base.{concept_field} AS concept_id,
            c.system,
            COUNT(*) AS row_count
        FROM {SOURCE_DATABASE}.{schema_name}.{table_name} base
        INNER JOIN {SOURCE_DATABASE}.OLIDS_TERMINOLOGY.CONCEPT_MAP cm
            ON base.{concept_field} = cm.source_code_id
        INNER JOIN {SOURCE_DATABASE}.OLIDS_TERMINOLOGY.CONCEPT c
            ON cm.target_code_id = c.id
        WHERE base.{concept_field} IS NOT NULL
            AND c.code IS NULL
        GROUP BY base.{concept_field}, c.system
        """)

    return "\nUNION ALL\n".join(queries)


def export_to_csv(df, filename, columns):
    """Export dataframe to CSV with specific columns, sorted by table then row_count descending."""
    if df.empty:
        return None

    output_file = OUTPUT_DIR / filename
    # Select only the columns we want and sort by table name, then row_count descending
    df_export = df[columns].copy()
    df_export = df_export.sort_values(['TABLE_NAME', 'ROW_COUNT'], ascending=[True, False])
    df_export.to_csv(output_file, index=False)
    return output_file


def main():
    """Execute concept mapping failure export."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    print(f"\n{'='*100}")
    print(f"CONCEPT MAPPING FAILURE EXPORT")
    print(f"{'='*100}")
    print(f"Database: {SOURCE_DATABASE}")
    print(f"Total fields checked: {len(CONCEPT_CHECKS)}")
    print(f"Output directory: {OUTPUT_DIR}")

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
    print("✓ Connected successfully\n")

    exported_files = []

    try:
        # ========================================
        # MISSING IN CONCEPT_MAP
        # ========================================
        print("Querying for concepts missing in CONCEPT_MAP...")
        query = build_missing_concept_map_query()
        df = session.sql(query).to_pandas()

        if not df.empty:
            filename = f"missing_concept_map_{timestamp}.csv"
            output_file = export_to_csv(df, filename, ['TABLE_NAME', 'CONCEPT_FIELD', 'CONCEPT_ID', 'ROW_COUNT'])
            print(f"✓ Exported {len(df):,} records to {filename}")
            exported_files.append(output_file)
        else:
            print("✓ No missing CONCEPT_MAP entries found")

        # ========================================
        # NULL DISPLAY WARNINGS
        # ========================================
        print("\nQuerying for concepts with NULL display...")
        query = build_null_display_query()
        df = session.sql(query).to_pandas()

        if not df.empty:
            filename = f"null_display_{timestamp}.csv"
            output_file = export_to_csv(df, filename, ['TABLE_NAME', 'CONCEPT_FIELD', 'CONCEPT_ID', 'CODE', 'SYSTEM', 'ROW_COUNT'])
            print(f"✓ Exported {len(df):,} records to {filename}")
            exported_files.append(output_file)
        else:
            print("✓ No NULL display warnings found")

        # ========================================
        # MISSING TARGET IN CONCEPT
        # ========================================
        print("\nQuerying for concepts with missing target in CONCEPT...")
        query = build_missing_target_concept_query()
        df = session.sql(query).to_pandas()

        if not df.empty:
            filename = f"missing_target_concept_{timestamp}.csv"
            output_file = export_to_csv(df, filename, ['TABLE_NAME', 'CONCEPT_FIELD', 'CONCEPT_ID', 'ROW_COUNT'])
            print(f"✓ Exported {len(df):,} records to {filename}")
            exported_files.append(output_file)
        else:
            print("✓ No missing target CONCEPT entries found")

        # ========================================
        # NULL CODE FAILURES
        # ========================================
        print("\nQuerying for concepts with NULL code...")
        query = build_null_code_query()
        df = session.sql(query).to_pandas()

        if not df.empty:
            filename = f"null_code_{timestamp}.csv"
            output_file = export_to_csv(df, filename, ['TABLE_NAME', 'CONCEPT_FIELD', 'CONCEPT_ID', 'SYSTEM', 'ROW_COUNT'])
            print(f"✓ Exported {len(df):,} records to {filename}")
            exported_files.append(output_file)
        else:
            print("✓ No NULL code failures found")

        # ========================================
        # SUMMARY
        # ========================================
        print(f"\n{'='*100}")
        print("EXPORT COMPLETE")
        print(f"{'='*100}")

        if exported_files:
            print(f"\nExported {len(exported_files)} file(s):")
            for file in exported_files:
                print(f"  • {file}")
        else:
            print("\n✓ No failures found - all concept mappings are valid!")

    finally:
        session.close()
        print("\nConnection closed")


if __name__ == '__main__':
    main()
