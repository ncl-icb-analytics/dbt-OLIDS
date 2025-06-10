{% macro test_staging_columns_match_source(source_schema, source_table, staging_model, source_columns) %}
-- Macro to check that all columns from the source table are present in the staging model
-- Column names are compared in lower case (as per staging convention)

with source_cols as (
    select lower(column_name) as column_name
    from (select unnest(array[{{ source_columns | map('lower') | join(", ") }}]) as column_name)
),

staging_cols as (
    select lower(column_name) as column_name
    from {{ ref(staging_model) }}.information_schema.columns
    where table_name = upper('{{ staging_model }}')
)

select
    '{{ source_table }}' as source_table,
    '{{ staging_model }}' as staging_model,
    s.column_name as missing_column
from source_cols s
left join staging_cols t on s.column_name = t.column_name
where t.column_name is null

{% endmacro %} 