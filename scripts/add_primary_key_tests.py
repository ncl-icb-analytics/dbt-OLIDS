"""
Generate schema.yml with primary key tests (unique and not_null) for all base models.
"""

from pathlib import Path
import yaml


def main():
    """Main execution."""
    base_dir = Path('models/olids/base')
    schema_path = base_dir / 'schema.yml'

    # Load existing schema
    with open(schema_path, 'r', encoding='utf-8') as f:
        schema = yaml.safe_load(f)

    # Add tests to each model
    for model in schema.get('models', []):
        # Check if columns already exist
        if 'columns' not in model:
            model['columns'] = []

        # Check if id column with tests already exists
        id_column_exists = any(col.get('name') == 'id' for col in model['columns'])

        if not id_column_exists:
            model['columns'].insert(0, {
                'name': 'id',
                'description': 'Primary key',
                'tests': ['unique', 'not_null']
            })

    # Write updated schema
    with open(schema_path, 'w', encoding='utf-8') as f:
        yaml.dump(schema, f, sort_keys=False, allow_unicode=True, width=1000, default_flow_style=False)

    print(f"[OK] Added primary key tests to {len(schema['models'])} base models")
    print(f"     Tests: unique, not_null on 'id' column")


if __name__ == '__main__':
    main()
