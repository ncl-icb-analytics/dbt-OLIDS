"""
Compare dbt sources.yml definitions against Snowflake INFORMATION_SCHEMA.
Identifies missing, extra, and type-mismatched columns per source table.
"""

import os
import yaml
from collections import defaultdict
from dotenv import load_dotenv
from snowflake.snowpark import Session

load_dotenv()

SOURCES_FILE = "models/sources.yml"

# Optional: remap a database name from sources.yml to a different physical DB
# at query time (e.g. testing against a renamed/replacement database).
# Set SOURCE_DB_OVERRIDE=NewDbName to redirect all sources.yml lookups.
DB_OVERRIDE = os.getenv("SOURCE_DB_OVERRIDE")

# Snowflake data type normalisation — maps INFORMATION_SCHEMA types to
# the shorthand forms used in dbt sources.yml
TYPE_MAP = {
    "VARCHAR": "TEXT",
    "TEXT": "TEXT",
    "STRING": "TEXT",
    "CHAR": "TEXT",
    "CHARACTER": "TEXT",
    "NUMBER": "NUMBER",
    "DECIMAL": "NUMBER",
    "NUMERIC": "NUMBER",
    "INT": "NUMBER",
    "INTEGER": "NUMBER",
    "BIGINT": "NUMBER",
    "SMALLINT": "NUMBER",
    "TINYINT": "NUMBER",
    "FLOAT": "FLOAT",
    "DOUBLE": "FLOAT",
    "REAL": "FLOAT",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "TIMESTAMP_NTZ": "TIMESTAMP_NTZ",
    "TIMESTAMP_LTZ": "TIMESTAMP_LTZ",
    "TIMESTAMP_TZ": "TIMESTAMP_TZ",
    "BINARY": "BINARY",
    "VARBINARY": "BINARY",
    "VARIANT": "VARIANT",
    "OBJECT": "OBJECT",
    "ARRAY": "ARRAY",
}


def normalise_type(sf_type: str) -> str:
    """Normalise a Snowflake data type to match dbt sources.yml conventions."""
    # Strip precision/scale e.g. NUMBER(38,0) -> NUMBER
    base = sf_type.split("(")[0].upper().strip()
    return TYPE_MAP.get(base, base)


def load_sources(path: str) -> dict:
    """Parse sources.yml and return {(database, schema, table): {col: type}}."""
    with open(path, "r") as f:
        data = yaml.safe_load(f)

    sources = {}
    for source in data.get("sources", []):
        # Strip quotes from database/schema identifiers
        db = source.get("database", "").strip('"')
        schema = source.get("schema", "").strip('"')

        for table in source.get("tables", []):
            table_name = table["name"].upper()
            columns = {}
            for col in table.get("columns", []):
                columns[col["name"].upper()] = col.get("data_type", "").upper()
            sources[(db, schema, table_name)] = columns

    return sources


def fetch_snowflake_columns(session: Session, database: str, schema: str, table: str) -> dict:
    """Query INFORMATION_SCHEMA for column names and types."""
    query = f"""
    SELECT column_name, data_type
    FROM "{database}".INFORMATION_SCHEMA.COLUMNS
    WHERE table_schema = '{schema}'
      AND table_name = '{table}'
    ORDER BY ordinal_position
    """
    df = session.sql(query).to_pandas()
    return {
        row["COLUMN_NAME"].upper(): normalise_type(row["DATA_TYPE"])
        for _, row in df.iterrows()
    }


def main():
    print("Loading sources.yml...")
    sources = load_sources(SOURCES_FILE)
    print(f"Found {len(sources)} source tables\n")

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

    issues_found = 0
    tables_checked = 0
    tables_ok = 0

    # Override only the OLIDS source DB (leave Dictionary etc. alone)
    OLIDS_DB_PREFIXES = ("Data_Store_OLIDS", "NCL_Data_Store_OLIDS")

    for (db, schema, table), dbt_cols in sorted(sources.items()):
        if DB_OVERRIDE and db.startswith(OLIDS_DB_PREFIXES):
            query_db = DB_OVERRIDE
        else:
            query_db = db
        sf_cols = fetch_snowflake_columns(session, query_db, schema, table)

        if not sf_cols:
            print(f"WARNING: {db}.{schema}.{table} — not found in Snowflake")
            issues_found += 1
            continue

        tables_checked += 1
        dbt_names = set(dbt_cols.keys())
        sf_names = set(sf_cols.keys())

        missing = sf_names - dbt_names  # in Snowflake but not in sources.yml
        extra = dbt_names - sf_names    # in sources.yml but not in Snowflake
        common = dbt_names & sf_names

        # Check type mismatches on shared columns
        type_mismatches = []
        for col in sorted(common):
            dbt_type = dbt_cols[col]
            sf_type = sf_cols[col]
            if dbt_type and sf_type and dbt_type != sf_type:
                type_mismatches.append((col, dbt_type, sf_type))

        if missing or extra or type_mismatches:
            print(f"{db}.{schema}.{table}")
            print("-" * 80)

            if missing:
                print(f"  IN SNOWFLAKE BUT NOT IN SOURCES.YML ({len(missing)}):")
                for col in sorted(missing):
                    print(f"    + {col} ({sf_cols[col]})")

            if extra:
                print(f"  IN SOURCES.YML BUT NOT IN SNOWFLAKE ({len(extra)}):")
                for col in sorted(extra):
                    print(f"    - {col} ({dbt_cols[col]})")

            if type_mismatches:
                print(f"  TYPE MISMATCHES ({len(type_mismatches)}):")
                for col, dbt_t, sf_t in type_mismatches:
                    print(f"    ~ {col}: sources.yml={dbt_t}, snowflake={sf_t}")

            print()
            issues_found += len(missing) + len(extra) + len(type_mismatches)
        else:
            tables_ok += 1

    session.close()

    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Tables checked: {tables_checked}")
    print(f"Tables fully aligned: {tables_ok}")
    print(f"Total issues: {issues_found}")


if __name__ == "__main__":
    main()
