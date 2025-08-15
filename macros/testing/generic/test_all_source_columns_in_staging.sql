{% test all_source_columns_in_staging(model) %}
    {# Generic test to verify all source columns are present in staging models #}
    {# Uses live information_schema to detect schema evolution #}

{%- set schema_prefixes = {
    'OLIDS_MASKED': 'stg_olids',
    'OLIDS_TERMINOLOGY': 'stg_olids_terminology',
    'CODESETS': 'stg_reference',
    'RULESETS': 'stg_reference',
    'REFERENCE': 'stg_reference'
} -%}

    {%- set ns = namespace(first=true) -%}

-- This test will fail if any source columns are missing from their corresponding staging models
-- Each row returned represents a missing column (detected from live source schema)

    {%- for source in sources -%}
    {%- set source_database = source.database -%}
    {%- set source_schema = source.schema -%}
    {%- set schema_clean = source_schema | replace('"', '') -%}
    {%- set prefix = schema_prefixes.get(schema_clean, 'stg') -%}

    {%- for table in source.tables -%}
        {%- set table_name = table.name -%}
        {%- set table_name_lower = table_name | lower -%}
        {%- set staging_model = prefix ~ '_' ~ table_name_lower -%}

        {%- if not ns.first -%}
            union all
        {%- endif -%}
        {%- set ns.first = false -%}

        {{ test_staging_columns_match_source(
            source_database,
            source_schema,
            table_name,
            staging_model
        ) }}

    {%- endfor -%}
{%- endfor -%}

    {%- if ns.first %}
-- Return empty result set if no sources found (test will pass)
select
    cast(null as string) as source_table,
    cast(null as string) as staging_model,
    cast(null as string) as missing_column,
    current_timestamp() as test_run_at
where 1=0
{%- endif -%}

{% endtest %}
