{% macro test_staging_columns_match_source(source_database, source_schema, source_table, staging_model) %}

-- Macro to check that all columns from the source table are present in the staging model
-- Uses live information_schema to detect schema evolution
-- Column names are compared in lower case (as per staging convention)

with source_cols as (
    select lower(column_name) as column_name
    from {{ source_database }}.information_schema.columns
    where table_schema = {{ source_schema }}
    and table_name = upper('{{ source_table }}')
),

staging_cols as (
    select lower(column_name) as column_name
    from {{ target.database }}.information_schema.columns
    where table_schema = upper('{{ target.schema }}')
    and table_name = upper('{{ staging_model }}')
)

select
    '{{ source_table }}' as source_table,
    '{{ staging_model }}' as staging_model,
    s.column_name as missing_column,
    current_timestamp() as test_run_at
from source_cols s
left join staging_cols t on s.column_name = t.column_name
where t.column_name is null

{% endmacro %}
