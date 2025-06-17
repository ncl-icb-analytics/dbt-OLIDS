{{
    config(
        materialized='table',
        tags=['data_quality', 'blood_pressure'],
        cluster_by=['person_id', 'clinical_effective_date'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Identifies blood pressure data quality issues: out-of-range values, orphaned readings, coding ambiguity, and missing dates. Supports clinical governance and data validation.'"
        ]
    )
}}

-- Blood Pressure Data Quality Issues
-- Identifies readings that are filtered out or do not pair properly in the main BP tables
-- Ensures alignment with our event-based BP consolidation approach

WITH base_observations_raw AS (
    -- Get ALL BP-related observations, including those with issues
    -- Keep NULL dates and out-of-range values for DQ analysis
    SELECT 
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date, -- Keep NULL dates
        obs.result_value,
        'mmHg' AS result_unit_display,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id
    FROM ({{ get_observations("'BP_COD', 'SYSBP_COD', 'DIABP_COD', 'HOMEAMBBP_COD', 'ABPM_COD', 'HOMEBP_COD'") }}) obs
    WHERE obs.result_value IS NOT NULL -- Still need a value to assess
    -- DO NOT filter dates or values here - we want to catch issues
),

row_flags_raw AS (
    -- Determine BP type for each observation (including problematic ones)
    SELECT 
        *,
        -- Flag for Systolic readings
        (source_cluster_id = 'SYSBP_COD' OR 
         (source_cluster_id = 'BP_COD' AND concept_display ILIKE '%systolic%')) AS is_systolic_row,
        
        -- Flag for Diastolic readings
        (source_cluster_id = 'DIABP_COD' OR 
         (source_cluster_id = 'BP_COD' AND concept_display ILIKE '%diastolic%')) AS is_diastolic_row
    FROM base_observations_raw
),

aggregated_raw_events AS (
    -- Aggregate per person/date (including NULL dates) without range filtering
    SELECT
        person_id,
        clinical_effective_date, -- Can be NULL
        
        -- Original values before filtering
        MAX(CASE WHEN is_systolic_row THEN result_value ELSE NULL END) AS systolic_value_original,
        MAX(CASE WHEN is_diastolic_row THEN result_value ELSE NULL END) AS diastolic_value_original,
        
        -- Metadata for traceability
        ANY_VALUE(result_unit_display) AS result_unit_display,
        ARRAY_AGG(DISTINCT observation_id) WITHIN GROUP (ORDER BY observation_id) AS all_observation_ids,
        ARRAY_AGG(DISTINCT concept_code) WITHIN GROUP (ORDER BY concept_code) AS all_concept_codes,
        ARRAY_AGG(DISTINCT concept_display) WITHIN GROUP (ORDER BY concept_display) AS all_concept_displays,
        ARRAY_AGG(DISTINCT source_cluster_id) WITHIN GROUP (ORDER BY source_cluster_id) AS all_source_cluster_ids,
        
        -- Flags for specific code presence (for ambiguity checking)
        BOOLOR_AGG(source_cluster_id = 'SYSBP_COD') AS had_sysbp_cod,
        BOOLOR_AGG(source_cluster_id = 'DIABP_COD') AS had_diabp_cod
        
    FROM row_flags_raw
    GROUP BY person_id, clinical_effective_date
    HAVING systolic_value_original IS NOT NULL OR diastolic_value_original IS NOT NULL
)

-- Final output: Only events with data quality issues
SELECT
    person_id,
    clinical_effective_date,
    systolic_value_original,
    diastolic_value_original,
    result_unit_display,
    all_concept_codes,
    all_concept_displays,
    all_source_cluster_ids,
    
    -- DQ Flag: Systolic out of range (< 40 or > 350)
    CASE 
        WHEN systolic_value_original IS NOT NULL 
             AND (systolic_value_original < 40 OR systolic_value_original > 350) 
        THEN TRUE 
        ELSE FALSE 
    END AS is_sbp_out_of_range,
    
    -- DQ Flag: Diastolic out of range (< 20 or > 200)
    CASE 
        WHEN diastolic_value_original IS NOT NULL 
             AND (diastolic_value_original < 20 OR diastolic_value_original > 200) 
        THEN TRUE 
        ELSE FALSE 
    END AS is_dbp_out_of_range,
    
    -- DQ Flag: Coding ambiguity (BP_COD without specific SYSBP/DIABP codes)
    CASE 
        WHEN ARRAY_CONTAINS('BP_COD'::VARIANT, all_source_cluster_ids) 
             AND NOT had_sysbp_cod AND NOT had_diabp_cod 
        THEN TRUE 
        ELSE FALSE 
    END AS is_coding_ambiguous,
    
    -- DQ Flag: Orphaned reading (SBP without DBP or vice versa)
    CASE 
        WHEN (systolic_value_original IS NOT NULL AND diastolic_value_original IS NULL) 
             OR (systolic_value_original IS NULL AND diastolic_value_original IS NOT NULL) 
        THEN TRUE 
        ELSE FALSE 
    END AS is_orphaned_reading,
    
    -- DQ Flag: Missing date
    CASE 
        WHEN clinical_effective_date IS NULL 
        THEN TRUE 
        ELSE FALSE 
    END AS is_date_missing

FROM aggregated_raw_events

-- Only include events with at least one DQ issue
WHERE (systolic_value_original IS NOT NULL AND (systolic_value_original < 40 OR systolic_value_original > 350))
   OR (diastolic_value_original IS NOT NULL AND (diastolic_value_original < 20 OR diastolic_value_original > 200))
   OR (ARRAY_CONTAINS('BP_COD'::VARIANT, all_source_cluster_ids) AND NOT had_sysbp_cod AND NOT had_diabp_cod)
   OR (systolic_value_original IS NOT NULL AND diastolic_value_original IS NULL)
   OR (systolic_value_original IS NULL AND diastolic_value_original IS NOT NULL)
   OR clinical_effective_date IS NULL

ORDER BY person_id, clinical_effective_date DESC 