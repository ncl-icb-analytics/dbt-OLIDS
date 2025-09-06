{{ config(
    materialized='table', 
    cluster_by=['indicator_id', 'cluster_id'],
    tags=['indicator_definitions'],
    post_hook="{{ archive_codes() }}"
) }}

/*
Indicator codes reference table.
Expands cluster IDs to individual SNOMED codes using stg_reference_combined_codesets.
Shows which specific codes define each indicator.
*/

{%- set metadata = extract_indicator_metadata() -%}

WITH cluster_definitions AS (
    {% if metadata.codes %}
        {% for code_ref in metadata.codes %}
        SELECT
            '{{ code_ref.indicator_id }}' AS indicator_id,
            '{{ code_ref.cluster_id }}' AS cluster_id,
            '{{ code_ref.code_category }}' AS code_category
        {% if not loop.last %}UNION ALL{% endif %}
        {% endfor %}
    {% else %}
        SELECT 
            NULL AS indicator_id,
            NULL AS cluster_id,
            NULL AS code_category
        WHERE FALSE  -- Empty result set
    {% endif %}
),

expanded_codes AS (
    -- Expand clusters to individual SNOMED codes only
    SELECT 
        cd.indicator_id,
        'SNOMED' AS code_system,
        cs.code,
        cs.code_description,
        cd.code_category,
        cd.cluster_id
    FROM cluster_definitions cd
    INNER JOIN {{ ref('stg_reference_combined_codesets') }} cs
        ON cd.cluster_id = cs.cluster_id
)

SELECT 
    indicator_id,
    cluster_id,
    code_category,
    code_system,
    code,
    code_description,
    CURRENT_TIMESTAMP() AS metadata_extracted_at
FROM expanded_codes
ORDER BY 
    indicator_id, 
    code_category, 
    cluster_id, 
    code