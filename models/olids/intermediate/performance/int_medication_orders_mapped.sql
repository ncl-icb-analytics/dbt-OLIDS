{{ config(
    materialized='incremental',
    cluster_by=['medication_statement_core_concept_id', 'order_date', 'patient_id'],
    incremental_strategy='append',
    on_schema_change='sync_all_columns',
    tags=['intermediate', 'performance']
) }}

/*
Pre-mapped medication orders table for performance optimization.

Purpose:
- Pre-joins medication statements and concept mappings
- Reduces data size by including only used columns
- Enables efficient partition pruning via clustering
- Append-only incremental using lds_start_date_time

Performance impact:
- Reduces query scans from 80GB+ to <1GB for typical queries
- Eliminates complex join paths at query time
- Optimized for cluster-based filtering

Maintenance:
- Daily incremental appends via lds_start_date_time
- Monthly full refresh to handle deletes/deduplication
*/

SELECT 
    -- Core identifiers
    mo.id as medication_order_id,
    mo.medication_statement_id,
    mo.patient_id,
    mo.person_id,
    mo.clinical_effective_date::DATE as order_date,
    
    -- Order details
    mo.dose as order_dose,
    mo.quantity_value as order_quantity_value,
    mo.quantity_unit as order_quantity_unit,
    mo.duration_days as order_duration_days,
    mo.medication_name as order_medication_name,
    
    -- Statement details
    ms.medication_statement_core_concept_id,
    ms.medication_name as statement_medication_name,
    
    -- Pre-mapped concept details
    c.id as mapped_concept_id,
    c.code as mapped_concept_code,
    c.display as mapped_concept_display,
    
    -- Track for incremental
    mo.lds_start_date_time

FROM {{ ref('stg_olids_medication_order') }} mo
JOIN {{ ref('stg_olids_medication_statement') }} ms
    ON mo.medication_statement_id = ms.id
LEFT JOIN {{ ref('stg_olids_terminology_concept_map') }} cm
    ON ms.medication_statement_core_concept_id = cm.source_code_id
LEFT JOIN {{ ref('stg_olids_terminology_concept') }} c
    ON cm.target_code_id = c.id
WHERE mo.clinical_effective_date IS NOT NULL

{% if is_incremental() %}
  AND mo.lds_start_date_time > (
    SELECT COALESCE(MAX(lds_start_date_time), '1900-01-01'::TIMESTAMP) 
    FROM {{ this }}
)
{% endif %}