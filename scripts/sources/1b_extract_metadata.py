
from snowflake.snowpark.session import Session
import pathlib
import os
import pandas as pd

print(os.getenv('SNOWFLAKE_ACCOUNT'))

def load_sql_query(file_path):
    """Load SQL query from a file."""
    with open(file_path, 'r') as file:
        return file.read()

def execute_sql_to_dataframe(session, sql_file_path):
    """
    Execute SQL query from file and return pandas DataFrame.
    
    Args:
        session: Snowpark session object
        sql_file_path: Path to the SQL file
    
    Returns:
        pandas.DataFrame: Results of the SQL query
    """
    # Load the SQL query from file
    sql_query = load_sql_query(sql_file_path)
    
    # Execute the query using session.sql()
    snowpark_df = session.sql(sql_query)
    
    # Convert Snowpark DataFrame to Pandas DataFrame
    pandas_df = snowpark_df.to_pandas()
    
    return pandas_df


if __name__ == "__main__":
    
    # Setup connection using snowpark API
    connection_params  = {
    "account" : os.getenv('SNOWFLAKE_ACCOUNT'),
    "user" : os.getenv('SNOWFLAKE_USER'),
    "authenticator" : "externalbrowser",
    "warehouse" :os.getenv('SNOWFLAKE_WAREHOUSE'),
    "role" : os.getenv('SNOWFLAKE_ROLE'),
    "database" : "MODELLING",
    "schema": "DBT_DEV"
    }

    session = Session.builder.configs(connection_params).create()

    print(f"role: {session.get_current_role()} | WH: {session.get_current_warehouse()} | DB.SCHEMA: {session.get_fully_qualified_current_schema()}")
    
    # File paths for query and output
    sql_file_path = pathlib.Path(__file__).parent / "metadata_query.sql"
    output_file = pathlib.Path.cwd() / "table_metadata.csv"
    
    # Execute query, get pandas DataFrame and write to root directory
    try:
        df = execute_sql_to_dataframe(session, sql_file_path)
        print("DataFrame created successfully!")
        print(f"Shape: {df.shape}")
        print(df.head())
        df.to_csv(output_file, index=False)
        print(f"\nMetadata extracted to {output_file}")
        print(f"\nNext step: Generate sources.yml file:")
        print(f"  python scripts/sources/2_generate_sources.py")
        
    except FileNotFoundError:
        print(f"SQL file not found: {sql_file_path}")
        print("Please run script 1a first to generate the metadata query:")
        print("  python scripts/sources/1a_generate_metadata_query.py")
    except Exception as e:
        print(f"Error executing query: {str(e)}")
    finally:
        session.close()