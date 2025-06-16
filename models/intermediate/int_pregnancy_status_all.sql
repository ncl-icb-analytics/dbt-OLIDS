{{
    config(
        materialized='table'
    )
}}

-- Pregnancy Status Intermediate Model (Data Collection Layer)
-- Collects ALL pregnancy-related observations using standardised pattern
-- Single Responsibility: Pregnancy observation data collection only

-- Use our standard macro for pregnancy observations
{{ get_observations("'PREG_COD', 'PREGDEL_COD'") }}

-- Additional transformations for pregnancy-specific flags
SELECT 
    person_id,
    clinical_effective_date,
    concept_code,
    concept_display,
    source_cluster_id,
    
    -- Pregnancy-specific flags (observation-level)
    CASE WHEN source_cluster_id = 'PREG_COD' THEN TRUE ELSE FALSE END AS is_pregnancy_code,
    CASE WHEN source_cluster_id = 'PREGDEL_COD' THEN TRUE ELSE FALSE END AS is_delivery_code,
    
    -- Standard metadata fields
    date_recorded,
    lds_datetime_data_acquired
    
FROM (
    {{ get_observations("'PREG_COD', 'PREGDEL_COD'") }}
) pregnancy_obs
WHERE source = 'UKHSA_FLU' -- Legacy specifies UKHSA_FLU source only 