{{ config(
    materialized='table',
    cluster_by=['indicator_id', 'sort_order', 'population_group'],
    tags=['indicator_definitions'],
    post_hook="{{ archive_thresholds() }}"
) }}

/*
Indicator thresholds reference table.
Expands threshold definitions from model YAML meta properties.
Shows population-specific targets, diagnostic criteria, and risk boundaries.
*/

{%- set metadata = extract_indicator_metadata() -%}

WITH threshold_definitions AS (
    {% if metadata.thresholds %}
        {% for threshold in metadata.thresholds %}
        SELECT
            '{{ threshold.indicator_id }}' AS indicator_id,
            '{{ threshold.population_group }}' AS population_group,
            '{{ threshold.threshold_type }}' AS threshold_type,
            '{{ threshold.threshold_value }}' AS threshold_value,
            '{{ threshold.threshold_operator }}' AS threshold_operator,
            '{{ threshold.threshold_unit }}' AS threshold_unit,
            '{{ threshold.description | replace("'", "''") }}' AS description,
            {{ threshold.sort_order | default(loop.index) }} AS sort_order,
            CURRENT_TIMESTAMP() AS metadata_extracted_at
        {% if not loop.last %}UNION ALL{% endif %}
        {% endfor %}
    {% else %}
        SELECT 
            NULL AS indicator_id,
            NULL AS population_group,
            NULL AS threshold_type,
            NULL AS threshold_value,
            NULL AS threshold_operator,
            NULL AS threshold_unit,
            NULL AS description,
            NULL AS sort_order,
            CURRENT_TIMESTAMP() AS metadata_extracted_at
        WHERE FALSE  -- Empty result set
    {% endif %}
)

SELECT * FROM threshold_definitions
ORDER BY 
    indicator_id, 
    sort_order,
    population_group,
    threshold_type