"""
Check referential integrity across OLIDS tables.
Identifies foreign key relationships (table_name + '_id' pattern) and validates that:
- All foreign key values exist in the referenced table's id column
- Reports orphaned records and broken references
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

# Schemas to check
SCHEMAS = ['OLIDS_COMMON', 'OLIDS_MASKED']


def get_all_tables(session):
    """Get all tables from OLIDS schemas."""
    print("\nDiscovering tables...")

    tables = []
    for schema in SCHEMAS:
        query = f"""
        SELECT table_schema, table_name
        FROM {SOURCE_DATABASE}.INFORMATION_SCHEMA.TABLES
        WHERE table_schema = '{schema}'
            AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
        result = session.sql(query).collect()
        for row in result:
            tables.append({
                'schema': row['TABLE_SCHEMA'],
                'name': row['TABLE_NAME']
            })

    print(f"✓ Found {len(tables)} tables across {len(SCHEMAS)} schemas")
    return tables


def get_table_columns(session, schema, table):
    """Get all columns for a specific table."""
    query = f"""
    SELECT column_name, data_type
    FROM {SOURCE_DATABASE}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_schema = '{schema}'
        AND table_name = '{table}'
    ORDER BY ordinal_position
    """
    result = session.sql(query).collect()
    return [{'name': row['COLUMN_NAME'], 'type': row['DATA_TYPE']} for row in result]


def find_foreign_key_relationships(tables, session):
    """Identify foreign key relationships based on naming convention (table_name + '_id')."""
    print("\nIdentifying foreign key relationships...")

    relationships = []

    # Create lookup dict for tables by lowercase name
    table_lookup = {t['name'].lower(): t for t in tables}

    for table in tables:
        columns = get_table_columns(session, table['schema'], table['name'])

        for col in columns:
            col_name = col['name']

            # Skip the primary key 'id' column
            if col_name.lower() == 'id':
                continue

            # Check if column follows foreign key pattern: <table_name>_id
            if col_name.lower().endswith('_id'):
                # Extract potential referenced table name
                potential_ref_table = col_name.lower()[:-3]  # Remove '_id' suffix

                # Check if this table exists
                if potential_ref_table in table_lookup:
                    ref_table = table_lookup[potential_ref_table]
                    relationships.append({
                        'child_schema': table['schema'],
                        'child_table': table['name'],
                        'fk_column': col_name,
                        'parent_schema': ref_table['schema'],
                        'parent_table': ref_table['name'],
                        'pk_column': 'ID'
                    })

    print(f"✓ Identified {len(relationships)} foreign key relationships")
    return relationships


def check_referential_integrity(session, relationship):
    """Check referential integrity for a single foreign key relationship."""
    child_schema = relationship['child_schema']
    child_table = relationship['child_table']
    fk_column = relationship['fk_column']
    parent_schema = relationship['parent_schema']
    parent_table = relationship['parent_table']
    pk_column = relationship['pk_column']

    query = f"""
    SELECT
        '{child_table}' AS child_table,
        '{fk_column}' AS fk_column,
        '{parent_table}' AS parent_table,
        COUNT(DISTINCT child.{fk_column}) AS total_distinct_fk_values,
        COALESCE(SUM(CASE WHEN child.{fk_column} IS NOT NULL THEN 1 ELSE 0 END), 0) AS total_rows_with_fk,
        COUNT(DISTINCT CASE WHEN child.{fk_column} IS NOT NULL AND parent.{pk_column} IS NULL THEN child.{fk_column} END) AS orphaned_fk_values,
        COALESCE(SUM(CASE WHEN child.{fk_column} IS NOT NULL AND parent.{pk_column} IS NULL THEN 1 ELSE 0 END), 0) AS orphaned_rows
    FROM {SOURCE_DATABASE}.{child_schema}.{child_table} child
    LEFT JOIN {SOURCE_DATABASE}.{parent_schema}.{parent_table} parent
        ON child.{fk_column} = parent.{pk_column}
    """

    result = session.sql(query).collect()
    return result[0] if result else None


def main():
    """Execute referential integrity checks."""
    print(f"\n{'='*100}")
    print(f"REFERENTIAL INTEGRITY CHECK")
    print(f"{'='*100}")
    print(f"Database: {SOURCE_DATABASE}")
    print(f"Schemas: {', '.join(SCHEMAS)}")

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
        # Get all tables
        tables = get_all_tables(session)

        # Find foreign key relationships
        relationships = find_foreign_key_relationships(tables, session)

        if not relationships:
            print("\n✓ No foreign key relationships found")
            return

        print(f"\nExecuting referential integrity checks (this may take several minutes)...")

        # Check each relationship
        results = []
        for i, rel in enumerate(relationships, 1):
            print(f"  Checking {i}/{len(relationships)}: {rel['child_table']}.{rel['fk_column']} → {rel['parent_table']}.{rel['pk_column']}")
            result = check_referential_integrity(session, rel)
            if result:
                results.append({
                    'child_table': result['CHILD_TABLE'],
                    'fk_column': result['FK_COLUMN'],
                    'parent_table': result['PARENT_TABLE'],
                    'total_distinct_fk': int(result['TOTAL_DISTINCT_FK_VALUES']),
                    'total_rows_with_fk': int(result['TOTAL_ROWS_WITH_FK']),
                    'orphaned_fk': int(result['ORPHANED_FK_VALUES']),
                    'orphaned_rows': int(result['ORPHANED_ROWS'])
                })

        print("✓ Checks completed\n")

        # Display results
        print("="*100)
        print("RESULTS")
        print("="*100)

        # Show broken relationships (orphaned records)
        broken = [r for r in results if r['orphaned_fk'] > 0]
        if broken:
            print(f"\n⚠️  BROKEN REFERENCES ({len(broken)} relationships):\n")
            for r in broken:
                orphan_pct = (r['orphaned_fk'] / r['total_distinct_fk'] * 100) if r['total_distinct_fk'] > 0 else 0
                print(f"  {r['child_table']}.{r['fk_column']} → {r['parent_table']}.ID")
                print(f"    Total distinct FK values: {r['total_distinct_fk']:,}")
                print(f"    Total rows with FK: {r['total_rows_with_fk']:,}")
                print(f"    Orphaned FK values: {r['orphaned_fk']:,} ({orphan_pct:.2f}%)")
                print(f"    Orphaned rows: {r['orphaned_rows']:,}")
                print()
        else:
            print("\n✓ No broken references detected!")

        # Show valid relationships
        valid = [r for r in results if r['orphaned_fk'] == 0]
        if valid:
            print(f"\n✓ VALID REFERENCES ({len(valid)} relationships):\n")
            for r in valid:
                print(f"  {r['child_table']}.{r['fk_column']} → {r['parent_table']}.ID")
                print(f"    Distinct FK values: {r['total_distinct_fk']:,}")
                print(f"    Total rows: {r['total_rows_with_fk']:,}")

        # Summary statistics
        print("\n" + "="*100)
        print("SUMMARY")
        print("="*100)
        total_relationships = len(results)
        total_broken = len(broken)
        total_valid = len(valid)
        total_orphaned_values = sum(r['orphaned_fk'] for r in results)
        total_orphaned_rows = sum(r['orphaned_rows'] for r in results)

        print(f"Total relationships checked: {total_relationships:,}")
        print(f"Broken references: {total_broken:,}")
        print(f"Valid references: {total_valid:,}")
        print(f"Total orphaned FK values: {total_orphaned_values:,}")
        print(f"Total orphaned rows: {total_orphaned_rows:,}")

    finally:
        session.close()
        print("\nConnection closed")


if __name__ == '__main__':
    main()
