import pandas as pd
import yaml
import os

# Default input and output paths
INPUT_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'table_metadata.csv')
OUTPUT_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'models', 'sources.yml')
DELIMITER = ','  # Use comma for CSV


def main():
    # Try comma first, fallback to tab if error
    try:
        df = pd.read_csv(INPUT_FILE, sep=',')
        df.columns = df.columns.str.strip()
        print("Columns found in file (comma):", df.columns.tolist())
        if len(df.columns) == 1:
            raise ValueError("Only one column found, trying tab delimiter.")
    except Exception:
        df = pd.read_csv(INPUT_FILE, sep='\t')
        df.columns = df.columns.str.strip()
        print("Columns found in file (tab):", df.columns.tolist())

    dbt_sources = {
        'version': 2,
        'sources': []
    }

    # Group by database and schema for dbt sources
    for (db, schema), tables in df.groupby(['DATABASE_NAME', 'SCHEMA_NAME']):
        dbt_sources['sources'].append({
            'name': schema.lower(),
            'database': db,
            'schema': schema,
            'tables': [
                {
                    'name': table,
                    'columns': [
                        {'name': col['COLUMN_NAME'], 'data_type': col['DATA_TYPE']}
                        for _, col in table_df.iterrows()
                    ]
                }
                for table, table_df in tables.groupby('TABLE_NAME')
            ]
        })

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        yaml.dump(dbt_sources, f, sort_keys=False, default_flow_style=False)

    print(f"YAML written to {OUTPUT_FILE}")

if __name__ == "__main__":
    main() 