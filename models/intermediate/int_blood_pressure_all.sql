{{
    config(
        materialized = 'table',
        tags = ['blood_pressure'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Consolidates blood pressure readings from multiple sources into a single comprehensive view per person per date. Features include:\n- Combines systolic and diastolic readings from the same date\n- Identifies readings taken at home or via ABPM (Ambulatory Blood Pressure Monitoring)\n- Validates readings against clinically acceptable ranges (systolic: 40-350 mmHg, diastolic: 20-200 mmHg)\n- Preserves traceability to source observations\n- Aggregates related concept codes and descriptions\n- Filters out NULL dates and implausible values\nThis table serves as the foundation for blood pressure-related indicators and clinical quality measures.'"
        ]
    )
}}

-- Consolidates all valid Blood Pressure (BP) readings (Systolic/Diastolic)
-- into single events per person per date.
-- Filters out readings with NULL dates or implausible values.
-- Determines context flags (Home/ABPM) based on associated codes.

WITH base_bp_readings AS (
    -- Get all BP-related observations using our observation macro
    -- This includes systolic, diastolic, and context readings
    {{ get_observations("'SYSBP_COD', 'DIABP_COD', 'BP_COD', 'HOMEBP_COD', 'HOMEAMBBP_COD', 'ABPM_COD'") }}
),

flagged_readings AS (
    SELECT 
        *,
        -- Flag for Systolic rows based on cluster ID or display text
        (cluster_id = 'SYSBP_COD' OR 
         (cluster_id = 'BP_COD' AND mapped_concept_display ILIKE '%systolic%')
        ) AS is_systolic_reading,
        
        -- Flag for Diastolic rows based on cluster ID or display text
        (cluster_id = 'DIABP_COD' OR 
         (cluster_id = 'BP_COD' AND mapped_concept_display ILIKE '%diastolic%')
        ) AS is_diastolic_reading,
        
        -- Context flags
        (cluster_id IN ('HOMEBP_COD', 'HOMEAMBBP_COD')) AS is_home_bp,
        (cluster_id = 'ABPM_COD') AS is_abpm_bp
    FROM base_bp_readings
    WHERE 
        -- Initial value validation
        clinical_effective_date IS NOT NULL
        AND result_value IS NOT NULL
        AND result_value > 20   -- Minimum plausible BP value
        AND result_value < 350  -- Maximum plausible BP value
),

consolidated_readings AS (
    SELECT
        person_id,
        sk_patient_id,
        clinical_effective_date::DATE AS bp_date,
        -- Get systolic/diastolic values
        MAX(CASE WHEN is_systolic_reading THEN result_value END) AS systolic_value,
        MAX(CASE WHEN is_diastolic_reading THEN result_value END) AS diastolic_value,
        -- Aggregate context flags using MAX instead of BOOL_OR
        MAX(CASE WHEN is_home_bp THEN TRUE ELSE FALSE END) AS is_home_bp_event,
        MAX(CASE WHEN is_abpm_bp THEN TRUE ELSE FALSE END) AS is_abpm_bp_event,
        -- Get observation IDs for traceability
        MAX(CASE WHEN is_systolic_reading THEN observation_id END) AS systolic_observation_id,
        MAX(CASE WHEN is_diastolic_reading THEN observation_id END) AS diastolic_observation_id,
        -- Collect all related codes and descriptions
        ARRAY_AGG(DISTINCT mapped_concept_code) WITHIN GROUP (ORDER BY mapped_concept_code) AS all_concept_codes,
        ARRAY_AGG(DISTINCT mapped_concept_display) WITHIN GROUP (ORDER BY mapped_concept_display) AS all_concept_displays,
        ARRAY_AGG(DISTINCT cluster_id) WITHIN GROUP (ORDER BY cluster_id) AS all_cluster_ids
    FROM flagged_readings
    GROUP BY 
        person_id,
        sk_patient_id,
        clinical_effective_date::DATE
)

SELECT *
FROM consolidated_readings
WHERE 
    -- Ensure at least one BP value exists
    (systolic_value IS NOT NULL OR diastolic_value IS NOT NULL)
    -- Apply specific range validation for each type
    AND (systolic_value IS NULL OR (systolic_value >= 40 AND systolic_value <= 350))
    AND (diastolic_value IS NULL OR (diastolic_value >= 20 AND diastolic_value <= 200)) 