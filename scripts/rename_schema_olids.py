"""
Rename DBT_DEV_OLIDS schema to OLIDS in DATA_LAB_OLIDS_NCL database.
This avoids reprocessing the entire pipeline.
"""
from snowflake.snowpark import Session
import os
from dotenv import load_dotenv


def main():
    """Main execution."""
    # Load environment variables
    load_dotenv()

    # Create Snowflake session
    connection_params = {
        "account": os.getenv('SNOWFLAKE_ACCOUNT'),
        "user": os.getenv('SNOWFLAKE_USER'),
        "authenticator": "externalbrowser",  # SSO
        "role": "ISL-USERGROUP-SECONDEES-NCL",
        "warehouse": os.getenv('SNOWFLAKE_WAREHOUSE'),
        "database": "DATA_LAB_OLIDS_NCL"
    }

    session = Session.builder.configs(connection_params).create()

    try:
        print("Checking current schemas...")

        # Check if DBT_DEV_OLIDS exists
        result = session.sql("SHOW SCHEMAS LIKE 'DBT_DEV_OLIDS'").collect()
        if not result:
            print("[ERROR] DBT_DEV_OLIDS schema does not exist")
            return

        print(f"[OK] Found DBT_DEV_OLIDS schema")

        # Check if OLIDS schema exists
        result = session.sql("SHOW SCHEMAS LIKE 'OLIDS'").collect()
        if result:
            print("[WARNING] OLIDS schema already exists - dropping it...")
            session.sql("DROP SCHEMA IF EXISTS OLIDS CASCADE").collect()
            print("[OK] Dropped OLIDS schema")

        # Rename DBT_DEV_OLIDS to OLIDS
        print("Renaming DBT_DEV_OLIDS to OLIDS...")
        session.sql("ALTER SCHEMA DBT_DEV_OLIDS RENAME TO OLIDS").collect()
        print("[OK] Successfully renamed DBT_DEV_OLIDS to OLIDS")

        # Verify the rename
        result = session.sql("SHOW SCHEMAS LIKE 'OLIDS'").collect()
        if result:
            print("[OK] Verified OLIDS schema exists")
        else:
            print("[ERROR] OLIDS schema not found after rename")

    finally:
        session.close()


if __name__ == '__main__':
    main()
