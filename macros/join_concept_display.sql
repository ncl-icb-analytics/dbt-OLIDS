{% macro join_concept_display(source_column, alias_prefix='') %}
    {%- if not source_column -%}
        {{ exceptions.raise_compiler_error("Must provide a source_column parameter to join_concept_display macro") }}
    {%- endif -%}
    
    {%- set source_alias = alias_prefix ~ 'source_concept' if alias_prefix else 'source_concept' -%}
    {%- set map_alias = alias_prefix ~ 'concept_map' if alias_prefix else 'concept_map' -%}
    {%- set target_alias = alias_prefix ~ 'target_concept' if alias_prefix else 'target_concept' -%}
    
    LEFT JOIN {{ ref('stg_olids_terminology_concept') }} AS {{ source_alias }}
        ON {{ source_column }} = {{ source_alias }}.id
    LEFT JOIN {{ ref('stg_olids_terminology_concept_map') }} AS {{ map_alias }}
        ON {{ source_column }} = {{ map_alias }}.source_code_id
    LEFT JOIN {{ ref('stg_olids_terminology_concept') }} AS {{ target_alias }}
        ON {{ map_alias }}.target_code_id = {{ target_alias }}.id
{% endmacro %}