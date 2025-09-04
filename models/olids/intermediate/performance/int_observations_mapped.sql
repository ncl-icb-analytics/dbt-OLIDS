{{ config(
    materialized='incremental',
    cluster_by=['observation_source_concept_id', 'clinical_effective_date', 'patient_id'],
    incremental_strategy='append',
    on_schema_change='sync_all_columns',
    tags=['intermediate', 'performance']
) }}

/*
Pre-mapped observations table for performance optimization.

Purpose:
- Pre-joins concept mappings to avoid expensive runtime joins
- Reduces data size by including only used columns
- Enables efficient partition pruning via clustering
- Append-only incremental using lds_start_date_time

Performance impact:
- Reduces query scans from 100GB+ to <1GB for typical queries
- Eliminates complex join paths at query time
- 40-50% size reduction vs full column set

Maintenance:
- Daily incremental appends via lds_start_date_time
- Monthly full refresh to handle deletes/deduplication
*/

SELECT 
    -- Core identifiers
    o.id as observation_id,
    o.patient_id,
    -- person_id excluded as it's rarely populated in source, use patient_person join instead
    o.clinical_effective_date::DATE as clinical_effective_date,
    
    -- Observation details
    o.observation_source_concept_id,
    o.result_value,
    o.result_value_unit_concept_id,
    o.result_text,
    
    -- Problem/review flags
    o.is_problem,
    o.is_review,
    o.problem_end_date::DATE as problem_end_date,
    
    -- Pre-mapped concept details
    c.id as mapped_concept_id,
    c.code as mapped_concept_code,
    c.display as mapped_concept_display,
    
    -- Pre-mapped unit concept
    unit_c.code as result_unit_code,
    unit_c.display as result_unit_display,
    
    -- Track for incremental
    o.lds_start_date_time

FROM {{ ref('stg_olids_observation') }} o
INNER JOIN {{ ref('stg_olids_terminology_concept_map') }} cm
    ON o.observation_source_concept_id = cm.source_code_id
INNER JOIN {{ ref('stg_olids_terminology_concept') }} c
    ON cm.target_code_id = c.id
-- Unit concept mapping (separate path, optional)
LEFT JOIN {{ ref('stg_olids_terminology_concept_map') }} unit_cm
    ON o.result_value_unit_concept_id = unit_cm.source_code_id
LEFT JOIN {{ ref('stg_olids_terminology_concept') }} unit_c
    ON unit_cm.target_code_id = unit_c.id

{% if is_incremental() %}
WHERE o.lds_start_date_time > (
    SELECT COALESCE(MAX(lds_start_date_time), '1900-01-01'::TIMESTAMP) 
    FROM {{ this }}
)
{% endif %}