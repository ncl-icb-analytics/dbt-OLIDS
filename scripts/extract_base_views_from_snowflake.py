"""
Extract base view definitions from Snowflake and convert to dbt models.

This script:
1. Connects to Snowflake using snowpark
2. Retrieves all view definitions from OLIDS_BASE schema using GET_DDL()
3. Extracts the SQL and converts it to dbt model format
4. Writes the models to the base layer directory
"""

from snowflake.snowpark.session import Session
import pathlib
import os
import re
from typing import Dict, Tuple


def clean_ddl_to_dbt_model(ddl: str, view_name: str) -> Tuple[str, str]:
    """
    Convert Snowflake DDL to dbt model SQL.

    Args:
        ddl: Full DDL from GET_DDL()
        view_name: Name of the view

    Returns:
        Tuple of (sql_content, comment_text)
    """
    # Extract the COMMENT clause
    comment_match = re.search(r"COMMENT='([^']*(?:''[^']*)*)'", ddl, re.DOTALL)
    comment_text = ""
    if comment_match:
        # Replace escaped quotes
        comment_text = comment_match.group(1).replace("''", "'")
        # Extract just the description part (before the emoji metadata)
        comment_parts = comment_text.split('\n\n🤖')
        comment_text = comment_parts[0].strip()

    # Extract the SELECT statement (everything after "as (")
    select_match = re.search(r'as\s+\(\s*(.*?)\s*\)\s*/\*', ddl, re.DOTALL | re.IGNORECASE)
    if not select_match:
        raise ValueError(f"Could not extract SELECT statement from DDL for {view_name}")

    sql_content = select_match.group(1).strip()

    # Clean up the SQL
    # Remove the trailing metadata comment if it exists
    sql_content = re.sub(r'\s*/\*.*?\*/\s*$', '', sql_content, flags=re.DOTALL)

    return sql_content, comment_text


def determine_config(sql_content: str, comment_text: str) -> str:
    """
    Determine the appropriate dbt config based on the SQL content.

    Args:
        sql_content: The SQL content
        comment_text: The comment describing the view

    Returns:
        Config block as string
    """
    # Check if it's a filtered view (has inner joins to patient/ncl_practices)
    has_patient_filter = 'base_olids_patient' in sql_content.lower()
    has_ncl_filter = 'ncl_practices' in sql_content.lower()

    if has_patient_filter and has_ncl_filter:
        # Filtered clinical view
        return """{{
    config(
        materialized='view',
        secure=true
    )
}}"""
    else:
        # Passthrough reference view
        return """{{
    config(
        materialized='view',
        secure=true
    )
}}"""


def source_ref_from_from_clause(from_clause: str) -> str:
    """
    Convert Snowflake fully qualified table name to dbt source reference.

    Args:
        from_clause: The FROM clause content

    Returns:
        dbt source reference
    """
    # Match patterns like: "Data_Store_OLIDS_Alpha"."OLIDS_MASKED"."TABLE_NAME"
    pattern = r'"Data_Store_OLIDS_Alpha[^"]*"\."(OLIDS_[^"]+)"\."([^"]+)"'

    def replace_match(match):
        schema = match.group(1)
        table = match.group(2)

        # Map schema to source name
        if schema == 'OLIDS_MASKED':
            source_name = 'olids_masked'
        elif schema == 'OLIDS_COMMON':
            source_name = 'olids_common'
        else:
            return match.group(0)  # Return unchanged if unknown schema

        return f"{{{{ source('{source_name}', '{table}') }}}}"

    return re.sub(pattern, replace_match, from_clause)


def convert_base_refs(sql_content: str) -> str:
    """
    Convert fully qualified base table references to dbt ref() calls.

    Args:
        sql_content: SQL content with FQ table names

    Returns:
        SQL with dbt ref() calls
    """
    # Pattern for: DATA_LAB_OLIDS_UAT.OLIDS_BASE.table_name
    pattern = r'DATA_LAB_OLIDS_UAT\.OLIDS_BASE\.([a-z_]+)'

    def replace_match(match):
        table_name = match.group(1)
        return f"{{{{ ref('base_olids_{table_name}') }}}}"

    return re.sub(pattern, replace_match, sql_content, flags=re.IGNORECASE)


def convert_staging_refs(sql_content: str) -> str:
    """
    Convert fully qualified staging table references to dbt ref() calls.

    Args:
        sql_content: SQL content with FQ table names

    Returns:
        SQL with dbt ref() calls
    """
    # Pattern for: DATA_LAB_OLIDS_UAT.DBT_DEV.stg_*
    pattern = r'DATA_LAB_OLIDS_UAT\.DBT_DEV\.(stg_[a-z_]+)'

    def replace_match(match):
        table_name = match.group(1)
        return f"{{{{ ref('{table_name}') }}}}"

    return re.sub(pattern, replace_match, sql_content, flags=re.IGNORECASE)


def create_dbt_model(view_name: str, ddl: str) -> str:
    """
    Create complete dbt model file content from DDL.

    Args:
        view_name: Name of the view
        ddl: Full DDL from Snowflake

    Returns:
        Complete dbt model file content
    """
    sql_content, comment_text = clean_ddl_to_dbt_model(ddl, view_name)

    # Convert source references
    sql_content = source_ref_from_from_clause(sql_content)

    # Convert base and staging references
    sql_content = convert_base_refs(sql_content)
    sql_content = convert_staging_refs(sql_content)

    # Generate config
    config = determine_config(sql_content, comment_text)

    # Build the complete model
    model_content = f"{config}\n\n{sql_content}"

    return model_content


def main():
    """Main execution."""
    # Setup connection
    connection_params = {
        "account": os.getenv('SNOWFLAKE_ACCOUNT'),
        "user": os.getenv('SNOWFLAKE_USER'),
        "authenticator": "externalbrowser",
        "warehouse": os.getenv('SNOWFLAKE_WAREHOUSE'),
        "role": "ISL-USERGROUP-SECONDEES-NCL",
        "database": "MODELLING",
        "schema": "DBT_DEV"
    }

    session = Session.builder.configs(connection_params).create()
    print(f"Connected: {session.get_fully_qualified_current_schema()}")

    # Get all views in OLIDS_BASE schema
    views_query = """
    SELECT table_name
    FROM DATA_LAB_OLIDS_UAT.information_schema.views
    WHERE table_schema = 'OLIDS_BASE'
    ORDER BY table_name
    """

    views_df = session.sql(views_query).to_pandas()
    view_names = views_df['TABLE_NAME'].tolist()

    print(f"\nFound {len(view_names)} views in OLIDS_BASE schema")

    # Output directory
    output_dir = pathlib.Path('models/olids/base')
    output_dir.mkdir(parents=True, exist_ok=True)

    # Extract each view
    success_count = 0
    failed_views = []

    for view_name in view_names:
        try:
            # Get DDL for the view
            ddl_query = f"SELECT GET_DDL('VIEW', 'DATA_LAB_OLIDS_UAT.OLIDS_BASE.{view_name}')"
            ddl_result = session.sql(ddl_query).collect()
            ddl = ddl_result[0][0]

            # Convert to dbt model
            model_content = create_dbt_model(view_name, ddl)

            # Write to file
            model_name = f"base_olids_{view_name.lower()}.sql"
            output_path = output_dir / model_name

            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(model_content)

            print(f"[OK] Extracted: {view_name} -> {model_name}")
            success_count += 1

        except Exception as e:
            print(f"[FAIL] Failed to extract {view_name}: {str(e)}")
            failed_views.append((view_name, str(e)))

    session.close()

    print(f"\n{'='*60}")
    print(f"Extraction complete: {success_count}/{len(view_names)} views extracted")

    if failed_views:
        print(f"\nFailed views ({len(failed_views)}):")
        for view_name, error in failed_views:
            print(f"  - {view_name}: {error}")

    print(f"\nModels written to: {output_dir}")
    print("\nNext steps:")
    print("  1. Review the extracted models")
    print("  2. Check that source() and ref() calls are correct")
    print("  3. Run: dbt compile")


if __name__ == '__main__':
    main()
