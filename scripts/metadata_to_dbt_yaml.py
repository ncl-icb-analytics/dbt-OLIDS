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

    # Group by database and schema to create sources
    sources = []
    for (database, schema), group in df.groupby(['DATABASE_NAME', 'SCHEMA_NAME']):
        tables = []
        for table_name, table_group in group.groupby('TABLE_NAME'):
            # Sort columns by ordinal position
            sorted_columns = table_group.sort_values('ORDINAL_POSITION')
            table = {
                'name': table_name,
                'columns': [{'name': col, 'data_type': dtype} for col, dtype in zip(sorted_columns['COLUMN_NAME'], sorted_columns['DATA_TYPE'])]
            }
            tables.append(table)

        source = {
            'name': schema,  # Use schema name as source name
            'database': f'"{database}"',  # Quote database name for case sensitivity
            'schema': schema,
            'tables': tables
        }
        sources.append(source)

    # Create sources.yml content
    sources_yml = {
        'version': 2,
        'sources': sources
    }

    # Write to file
    with open(OUTPUT_FILE, 'w') as f:
        yaml.dump(sources_yml, f, sort_keys=False, default_flow_style=False)
        print(f"Generated {OUTPUT_FILE}")


if __name__ == '__main__':
    main() 