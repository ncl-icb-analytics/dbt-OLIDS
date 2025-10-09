"""
Add data quality checks to base models:
1. Filter out deleted records (lds_is_deleted = TRUE)
2. Add deduplication logic using ROW_NUMBER() where needed
3. Generate schema tests for primary keys (unique and not_null on id)
"""

from pathlib import Path
import re
import yaml

# Models that don't have lds_is_deleted column (reference tables)
NO_DELETION_FLAG = {
    'ncl_practices',
    'postcode_hash',
}

# Models that need deduplication (transactional tables that might have duplicates)
NEEDS_DEDUPLICATION = {
    'observation',
    'medication_order',
    'medication_statement',
    'allergy_intolerance',
    'diagnostic_order',
    'procedure_request',
    'referral_request',
    'encounter',
    'flag',
    'appointment',
}


def add_deletion_filter(sql_content: str, model_name: str) -> str:
    """Add lds_is_deleted filter to WHERE clause."""
    if model_name in NO_DELETION_FLAG:
        return sql_content

    # Check if there's already a WHERE clause
    if 'WHERE' in sql_content:
        # Add to existing WHERE
        sql_content = re.sub(
            r'(WHERE\s+)',
            r'\1src."lds_is_deleted" = FALSE\n    AND ',
            sql_content,
            count=1
        )
    else:
        # Add new WHERE clause before the FROM if no WHERE exists
        # This applies to models without sensitive code filtering
        sql_content = re.sub(
            r'(FROM\s+{{.*?}}.*?)(\s*$)',
            r'\1\nWHERE src."lds_is_deleted" = FALSE\2',
            sql_content,
            flags=re.DOTALL
        )

    return sql_content


def add_deduplication(sql_content: str, model_name: str) -> str:
    """Add ROW_NUMBER() deduplication logic."""
    if model_name not in NEEDS_DEDUPLICATION:
        return sql_content

    # Check if already has ROW_NUMBER
    if 'ROW_NUMBER()' in sql_content:
        return sql_content

    # Wrap the current query in a CTE with ROW_NUMBER
    # Extract the SELECT...FROM...WHERE block
    config_match = re.search(r'({{.*?}}\s*)', sql_content, re.DOTALL)
    if not config_match:
        return sql_content

    config_block = config_match.group(1)
    query_block = sql_content[len(config_block):].strip()

    # Create deduplicated version
    deduplicated = f"""{config_block}

WITH source_data AS (
{query_block}
),

deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY lds_start_date_time DESC, lds_lakehouse_datetime_updated DESC
        ) AS row_num
    FROM source_data
)

SELECT
    {', '.join([col.strip() for col in re.findall(r'(\w+)(?:,|\s+FROM)', query_block) if col != 'FROM'])}
FROM deduplicated
WHERE row_num = 1
"""

    return deduplicated


def generate_schema_tests() -> dict:
    """Generate schema.yml with primary key tests for all base models."""
    base_dir = Path('models/olids/base')

    models = []
    for sql_file in sorted(base_dir.glob('base_olids_*.sql')):
        model_name = sql_file.stem

        # Read existing description from schema.yml
        schema_path = base_dir / 'schema.yml'
        description = f"{model_name.replace('base_olids_', '').replace('_', ' ').title()} base view"

        with open(schema_path, 'r', encoding='utf-8') as f:
            existing_schema = yaml.safe_load(f)
            for model in existing_schema.get('models', []):
                if model['name'] == model_name:
                    description = model.get('description', description)
                    break

        # Add model with tests
        model_config = {
            'name': model_name,
            'description': description,
            'columns': [
                {
                    'name': 'id',
                    'description': 'Primary key',
                    'tests': ['unique', 'not_null']
                }
            ]
        }

        models.append(model_config)

    return {
        'version': 2,
        'models': models
    }


def main():
    """Main execution."""
    base_dir = Path('models/olids/base')

    print("Adding data quality checks to base models...\n")

    updated = []

    for sql_file in sorted(base_dir.glob('base_olids_*.sql')):
        model_name = sql_file.stem.replace('base_olids_', '')

        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()

        original_content = sql_content

        # Add deletion filter
        sql_content = add_deletion_filter(sql_content, model_name)

        # Note: Deduplication would require significant refactoring of each query
        # Better to handle in a separate pass or case-by-case basis

        if sql_content != original_content:
            with open(sql_file, 'w', encoding='utf-8') as f:
                f.write(sql_content)
            updated.append(model_name)
            print(f"[OK] {model_name}: Added deletion filter")

    # Generate schema tests
    schema = generate_schema_tests()
    schema_path = base_dir / 'schema.yml'

    with open(schema_path, 'w', encoding='utf-8') as f:
        yaml.dump(schema, f, sort_keys=False, allow_unicode=True, width=1000, default_flow_style=False)

    print(f"\n[OK] Generated schema.yml with primary key tests for {len(schema['models'])} models")

    print(f"\n{'='*60}")
    print(f"Updated {len(updated)} base models with deletion filtering")
    print(f"\nNote: Deduplication logic should be added case-by-case")
    print(f"      Consider using QUALIFY ROW_NUMBER() for models with duplicates")


if __name__ == '__main__':
    main()
