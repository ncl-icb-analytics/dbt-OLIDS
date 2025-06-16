{{
    config(
        materialized='table',
        tags=['intermediate', 'clinical', 'blood_pressure'],
        cluster_by=['person_id', 'clinical_effective_date'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'All blood pressure readings consolidated from OLIDS observations. Includes systolic, diastolic and combined BP readings with clinical validation. Contains ALL persons (active and inactive) for comprehensive analysis.'"
        ]
    )
}}

-- Blood Pressure Observations - All readings for ALL persons
-- Consolidates systolic, diastolic and combined BP readings from OLIDS
-- Includes all persons (active/inactive, deceased, etc.) for complete intermediate data

WITH bp_observations AS (
    -- Get all blood pressure observations using our standard macro
    SELECT 
        obs.*,
        -- Validate clinical ranges
        CASE 
            WHEN obs.result_value >= 40 AND obs.result_value <= 350 
            THEN obs.result_value 
            ELSE NULL 
        END AS validated_systolic,
        CASE 
            WHEN obs.result_value >= 20 AND obs.result_value <= 200 
            THEN obs.result_value 
            ELSE NULL 
        END AS validated_diastolic
    FROM (
        {{ get_observations("'SYSBP_COD', 'DIABP_COD', 'BP_COD'") }}
    ) obs
    WHERE obs.clinical_effective_date <= CURRENT_DATE() -- No future dates
),

bp_typed AS (
    -- Categorise BP readings by type
    SELECT 
        *,
        CASE 
            WHEN cluster_id = 'SYSBP_COD' THEN 'Systolic'
            WHEN cluster_id = 'DIABP_COD' THEN 'Diastolic' 
            WHEN cluster_id = 'BP_COD' THEN 'Combined'
            ELSE 'Unknown'
        END AS bp_type,
        -- Apply appropriate validation based on type
        CASE 
            WHEN cluster_id = 'SYSBP_COD' THEN validated_systolic
            WHEN cluster_id = 'DIABP_COD' THEN validated_diastolic
            WHEN cluster_id = 'BP_COD' THEN result_value -- Combined readings may have different ranges
            ELSE result_value
        END AS validated_value
    FROM bp_observations
)

-- Final selection with ALL persons - no filtering by active status
-- Downstream models can filter as needed for their specific use cases
SELECT 
    bp.*,
    -- Add basic person demographics for reference (but include ALL persons)
    p.current_practice_id,
    p.total_patients
FROM bp_typed bp
-- Join to main person dimension (includes ALL persons)
LEFT JOIN {{ ref('dim_person') }} p
    ON bp.person_id = p.person_id
WHERE bp.validated_value IS NOT NULL -- Only valid readings
ORDER BY bp.person_id, bp.clinical_effective_date DESC 