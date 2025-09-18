{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif custom_schema_name == 'olids_base' -%}
        OLIDS_BASE
    {%- elif custom_schema_name == 'olids_stable' -%}
        OLIDS_STABLE
    {%- elif custom_schema_name == 'dbt_base' -%}
        OLIDS_BASE
    {%- else -%}
        -- Default behavior for other custom schemas
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}