"""
Find tables present in the OLIDS database but missing from sources.yml.
Complements validate_source_schema.py, which only checks declared tables.

Set SOURCE_DB_OVERRIDE to query a different physical database than the one
declared in sources.yml (e.g. during a DB rename migration).
"""

import os
import yaml
from collections import defaultdict
from dotenv import load_dotenv
from snowflake.snowpark import Session

load_dotenv()

SOURCES_FILE = "models/sources.yml"
DB_OVERRIDE = os.getenv("SOURCE_DB_OVERRIDE")
OLIDS_DB_PREFIXES = ("Data_Store_OLIDS", "NCL_Data_Store_OLIDS")


def load_declared_tables(path: str) -> dict:
    """Return {(database, schema): {table_names}} declared in sources.yml."""
    with open(path, "r") as f:
        data = yaml.safe_load(f)

    declared = defaultdict(set)
    for source in data.get("sources", []):
        db = source.get("database", "").strip('"')
        schema = source.get("schema", "").strip('"')
        for table in source.get("tables", []):
            declared[(db, schema)].add(table["name"].upper())
    return declared


def fetch_db_tables(session: Session, database: str, schema: str) -> dict:
    """Return {table_name: table_type} for all tables/views in a schema."""
    query = f"""
    SELECT table_name, table_type
    FROM "{database}".INFORMATION_SCHEMA.TABLES
    WHERE table_schema = '{schema}'
    ORDER BY table_name
    """
    df = session.sql(query).to_pandas()
    return {row["TABLE_NAME"].upper(): row["TABLE_TYPE"] for _, row in df.iterrows()}


def fetch_db_schemas(session: Session, database: str) -> list:
    """Return all non-system schemas in a database."""
    query = f"""
    SELECT schema_name
    FROM "{database}".INFORMATION_SCHEMA.SCHEMATA
    WHERE schema_name NOT IN ('INFORMATION_SCHEMA')
    ORDER BY schema_name
    """
    df = session.sql(query).to_pandas()
    return df["SCHEMA_NAME"].tolist()


def main():
    print("Loading sources.yml...")
    declared = load_declared_tables(SOURCES_FILE)
    olids_schemas = [
        (db, schema) for (db, schema) in declared
        if db.startswith(OLIDS_DB_PREFIXES)
    ]
    declared_schema_names = {schema for (_, schema) in olids_schemas}
    print(f"Found {len(olids_schemas)} OLIDS (db, schema) pairs declared\n")

    print("Connecting to Snowflake...")
    authenticator = os.getenv("SNOWFLAKE_AUTHENTICATOR", "externalbrowser")
    sf_config = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "authenticator": authenticator,
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
    }
    if authenticator != "externalbrowser" and os.getenv("SNOWFLAKE_PASSWORD"):
        sf_config["password"] = os.getenv("SNOWFLAKE_PASSWORD")
    session = Session.builder.configs(sf_config).create()
    print(f"Connected as {session.get_current_user()}\n")

    # Discover schemas in the actual DB and flag undeclared ones
    physical_db = DB_OVERRIDE or olids_schemas[0][0]
    db_schemas = fetch_db_schemas(session, physical_db)
    undeclared_schemas = [s for s in db_schemas if s not in declared_schema_names]

    print(f"Schemas present in {physical_db}: {len(db_schemas)}")
    print(f"  declared : {sorted(declared_schema_names)}")
    print(f"  present  : {sorted(db_schemas)}")
    if undeclared_schemas:
        print(f"\n  SCHEMAS IN DB BUT NOT IN SOURCES.YML ({len(undeclared_schemas)}):")
        for s in sorted(undeclared_schemas):
            tables = fetch_db_tables(session, physical_db, s)
            print(f"    + {s}  ({len(tables)} tables/views)")
            for t, ttype in sorted(tables.items()):
                print(f"        - {t}  ({ttype})")
    print()

    total_missing = 0
    for db, schema in sorted(olids_schemas):
        query_db = DB_OVERRIDE if DB_OVERRIDE else db
        db_tables = fetch_db_tables(session, query_db, schema)
        declared_tables = declared[(db, schema)]

        in_db_only = set(db_tables.keys()) - declared_tables
        in_yml_only = declared_tables - set(db_tables.keys())

        header = f"{query_db}.{schema}"
        print(header)
        print("-" * len(header))
        print(f"  declared in sources.yml : {len(declared_tables)}")
        print(f"  present in database     : {len(db_tables)}")

        if in_db_only:
            print(f"\n  IN DB BUT NOT IN SOURCES.YML ({len(in_db_only)}):")
            for t in sorted(in_db_only):
                print(f"    + {t}  ({db_tables[t]})")
            total_missing += len(in_db_only)

        if in_yml_only:
            print(f"\n  IN SOURCES.YML BUT NOT IN DB ({len(in_yml_only)}):")
            for t in sorted(in_yml_only):
                print(f"    - {t}")

        print()

    session.close()

    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total tables in DB but missing from sources.yml: {total_missing}")


if __name__ == "__main__":
    main()
