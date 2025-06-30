import yaml
import os

# Paths
SOURCES_YML = os.path.join('models', 'sources.yml')
STAGING_DIR = os.path.join('models', 'staging')

# Schema to prefix mapping
SCHEMA_PREFIXES = {
    'OLIDS_MASKED': 'stg_olids',
    'OLIDS_TERMINOLOGY': 'stg_olids_term',
    'CODESETS': 'stg_codesets',
    'RULESETS': 'stg_rulesets'
}

# Load sources.yml
with open(SOURCES_YML) as f:
    sources = yaml.safe_load(f)

os.makedirs(STAGING_DIR, exist_ok=True)

for source in sources['sources']:
    schema = source['schema']
    prefix = SCHEMA_PREFIXES.get(schema, 'stg')  # Default to 'stg' if schema not in mapping

    for table in source['tables']:
        # Keep original case for source reference
        table_name = table['name']
        # Use lowercase for file names and column references
        table_name_lower = table_name.lower()
        columns = [col['name'] for col in table.get('columns', [])]
        if not columns:
            continue  # Skip tables with no columns listed

        # Quote source columns but expose them as lowercase without quotes
        column_list = ',\n    '.join(f'"{col}" as {col.lower()}' for col in columns)
        model_sql = f"-- Staging model for {schema}.{table_name}\n"
        model_sql += f"-- Source: {source['database']}.{schema}\n\n"
        model_sql += f"select\n    {column_list}\nfrom {{{{ source('{schema}', '{table_name}') }}}}"

        # Create model name with prefix
        model_name = f"{prefix}_{table_name_lower}"
        out_path = os.path.join(STAGING_DIR, f'{model_name}.sql')

        with open(out_path, 'w') as out_f:
            out_f.write(model_sql + '\n')
