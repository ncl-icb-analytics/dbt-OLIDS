{% macro get_latest_events(from_table, partition_by='person_id', order_by='clinical_effective_date', direction='DESC') %}
    {#- Get latest events for each person from a set of events -#}
    {#- Uses Snowflake's QUALIFY statement for efficient filtering of window functions -#}

    {%- if partition_by is string -%}
        {%- set partition_cols = partition_by -%}
    {%- elif partition_by is iterable -%}
        {%- set partition_cols = partition_by | join(', ') -%}
    {%- else -%}
        {%- set partition_cols = partition_by -%}
    {%- endif -%}

    {%- if order_by is string -%}
        {%- set order_cols = order_by -%}
    {%- elif order_by is iterable -%}
        {%- set order_cols = order_by | join(', ') -%}
    {%- else -%}
        {%- set order_cols = order_by -%}
    {%- endif -%}

SELECT *
FROM {{ from_table }}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY {{ partition_cols }}
    ORDER BY {{ order_cols }} {{ direction }}
) = 1
{% endmacro %}
