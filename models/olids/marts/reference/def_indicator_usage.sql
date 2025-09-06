{{ config(
    materialized='table',
    tags=['indicator_definitions'],
    post_hook="{{ archive_usage() }}"
) }}

/*
Indicator usage reference table.
Tracks where each indicator is used (dashboards, reports, etc.).
Simple mapping without additional details.
*/

{%- set metadata = extract_indicator_metadata() -%}

WITH usage_contexts AS (
    {% if metadata.usage %}
        {% for usage in metadata.usage %}
        SELECT
            '{{ usage.indicator_id }}' AS indicator_id,
            '{{ usage.usage_context }}' AS usage_context
        {% if not loop.last %}UNION ALL{% endif %}
        {% endfor %}
    {% else %}
        SELECT 
            NULL AS indicator_id,
            NULL AS usage_context
        WHERE FALSE  -- Empty result set
    {% endif %}
)

SELECT DISTINCT 
    indicator_id,
    usage_context,
    CURRENT_TIMESTAMP() AS metadata_extracted_at
FROM usage_contexts
ORDER BY indicator_id, usage_context