{{ config(materialized='view') }}

/*
Summary view combining indicators with usage and code counts.
Easy-to-use view for testing and dashboard consumption.
*/

WITH usage_summary AS (
    SELECT 
        indicator_id,
        LISTAGG(usage_context, ', ') WITHIN GROUP (ORDER BY usage_context) AS usage_contexts,
        MAX(CASE WHEN usage_context = 'POPULATION_HEALTH_NEEDS_DASHBOARD' THEN TRUE ELSE FALSE END) AS in_pop_health_dashboard,
        MAX(CASE WHEN usage_context = 'QOF' THEN TRUE ELSE FALSE END) AS in_qof
    FROM {{ ref('def_indicator_usage') }}
    GROUP BY indicator_id
),

code_summary AS (
    SELECT 
        indicator_id,
        COUNT(DISTINCT CASE WHEN code_system = 'SNOMED' AND code_category = 'INCLUSION' THEN code END) AS inclusion_code_count,
        COUNT(DISTINCT CASE WHEN code_system = 'SNOMED' AND code_category = 'RESOLUTION' THEN code END) AS resolution_code_count,
        COUNT(DISTINCT cluster_id) AS cluster_count
    FROM {{ ref('def_indicator_codes') }}
    GROUP BY indicator_id
)

SELECT 
    i.indicator_id,
    i.indicator_type,
    i.category,
    i.clinical_domain,
    i.name_short,
    i.description_short,
    i.source_model,
    i.source_column,
    i.is_qof,
    i.qof_indicator,
    u.usage_contexts,
    u.in_pop_health_dashboard,
    u.in_qof,
    COALESCE(c.inclusion_code_count, 0) AS inclusion_code_count,
    COALESCE(c.resolution_code_count, 0) AS resolution_code_count,
    COALESCE(c.cluster_count, 0) AS cluster_count,
    i.metadata_extracted_at
FROM {{ ref('def_indicator') }} i
LEFT JOIN usage_summary u ON i.indicator_id = u.indicator_id
LEFT JOIN code_summary c ON i.indicator_id = c.indicator_id
ORDER BY i.sort_order, i.indicator_id