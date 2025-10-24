"""
Query Snowflake information schema to discover table structures.
Uses SSO authentication from .env file.
"""

import os
from dotenv import load_dotenv
from snowflake.snowpark import Session
import pandas as pd

# Load environment variables
load_dotenv()

# Create Snowflake session with SSO
connection_parameters = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "authenticator": "externalbrowser",  # SSO
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "role": os.getenv("SNOWFLAKE_ROLE"),
}

print("Connecting to Snowflake...")
session = Session.builder.configs(connection_parameters).create()
print(f"Connected as {session.get_current_user()}")

# Databases and schemas to query
targets = [
    ("Data_Store_OLIDS_Alpha", "OLIDS_REFERENCE"),
    ("Data_Store_OLIDS_Alpha", "OLIDS_MASKED"),
    ("Data_Store_OLIDS_Alpha", "OLIDS_TERMINOLOGY"),
    ("Dictionary", "dbo"),
]

for database, schema in targets:
    print(f"\n{'='*80}")
    print(f"Database: {database}, Schema: {schema}")
    print(f"{'='*80}")

    try:
        # Get list of tables
        tables_query = f"""
        SELECT table_name, table_type
        FROM "{database}".INFORMATION_SCHEMA.TABLES
        WHERE table_schema = '{schema}'
        ORDER BY table_name
        """
        tables_df = session.sql(tables_query).to_pandas()

        if tables_df.empty:
            print(f"No tables found in {database}.{schema}")
            continue

        print(f"\nFound {len(tables_df)} tables:")
        for idx, row in tables_df.iterrows():
            print(f"  - {row['TABLE_NAME']} ({row['TABLE_TYPE']})")

        # For OLIDS_REFERENCE, get detailed column info for all tables
        if schema == "OLIDS_REFERENCE":
            for table_name in tables_df['TABLE_NAME']:
                print(f"\n{'-'*80}")
                print(f"Table: {database}.{schema}.{table_name}")
                print(f"{'-'*80}")

                columns_query = f"""
                SELECT
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM "{database}".INFORMATION_SCHEMA.COLUMNS
                WHERE table_schema = '{schema}'
                  AND table_name = '{table_name}'
                ORDER BY ordinal_position
                """
                columns_df = session.sql(columns_query).to_pandas()

                print(columns_df.to_string(index=False))

    except Exception as e:
        print(f"Error querying {database}.{schema}: {e}")

session.close()
print("\nDone!")
