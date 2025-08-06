{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All HbA1c measurements from observations.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Handles both IFCC and DCCT measurement types with proper unit tracking.
*/

WITH base_observations AS (

    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        -- Handle extreme values gracefully - cap at reasonable HbA1c ranges
        CASE 
            WHEN TRY_CAST(obs.result_value AS FLOAT) > 1000 THEN NULL  -- Likely data error
            WHEN TRY_CAST(obs.result_value AS FLOAT) < 0 THEN NULL     -- Negative values invalid
            ELSE CAST(obs.result_value AS NUMBER(10,2))
        END AS hba1c_value,
        obs.result_unit_display,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        obs.result_value AS original_result_value,

        -- Single measurement type determination - mutually exclusive
        -- Priority: 1) Cluster ID (most reliable), 2) Unit display, 3) Value range
        CASE
            WHEN obs.cluster_id = 'IFCCHBAM_COD' THEN 'IFCC'
            WHEN obs.cluster_id = 'DCCTHBA1C_COD' THEN 'DCCT'
            WHEN obs.result_unit_display ILIKE '%mmol/mol%' THEN 'IFCC'
            WHEN obs.result_unit_display ILIKE '%\%%' THEN 'DCCT'
            WHEN obs.result_value > 25 THEN 'IFCC'  -- IFCC values typically >25
            WHEN obs.result_value <= 25 THEN 'DCCT' -- DCCT values typically <=25
            ELSE 'UNKNOWN'
        END AS measurement_type
    FROM ({{ get_observations("'IFCCHBAM_COD', 'DCCTHBA1C_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
      AND obs.result_value IS NOT NULL
)

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    hba1c_value,
    result_unit_display,
    concept_code,
    concept_display,
    source_cluster_id,
    measurement_type,
    (measurement_type = 'IFCC') AS is_ifcc,
    (measurement_type = 'DCCT') AS is_dcct,
    original_result_value,

    -- Use actual result unit display from source data (enhanced macro)

    -- Enhanced result string for reporting (value + unit from source)
    CASE
        WHEN result_unit_display IS NOT NULL
        THEN CAST(hba1c_value AS VARCHAR) || ' ' || result_unit_display
        ELSE CAST(hba1c_value AS VARCHAR)
    END AS hba1c_result_display,

    -- Data quality validation (enhanced range checks)
    CASE
        WHEN measurement_type = 'IFCC' AND hba1c_value BETWEEN 20 AND 200 THEN TRUE  -- IFCC: mmol/mol (expanded range)
        WHEN measurement_type = 'DCCT' AND hba1c_value BETWEEN 3 AND 20 THEN TRUE    -- DCCT: %
        ELSE FALSE
    END AS is_valid_hba1c,

    -- Enhanced clinical categorisation with diabetes diagnostic thresholds
    CASE
        -- IFCC (mmol/mol) categories
        WHEN measurement_type = 'IFCC' AND hba1c_value < 42 THEN 'Normal'           -- <6.0%
        WHEN measurement_type = 'IFCC' AND hba1c_value BETWEEN 42 AND 47 THEN 'Prediabetes'  -- 6.0-6.4%
        WHEN measurement_type = 'IFCC' AND hba1c_value BETWEEN 48 AND 52 THEN 'Diabetes - Target'     -- 6.5-6.9%
        WHEN measurement_type = 'IFCC' AND hba1c_value BETWEEN 53 AND 57 THEN 'Diabetes - Acceptable' -- 7.0-7.4%
        WHEN measurement_type = 'IFCC' AND hba1c_value BETWEEN 58 AND 63 THEN 'Diabetes - Above Target'  -- 7.5-7.9%
        WHEN measurement_type = 'IFCC' AND hba1c_value BETWEEN 64 AND 85 THEN 'Diabetes - Poor Control'  -- 8.0-9.9%
        WHEN measurement_type = 'IFCC' AND hba1c_value >= 86 THEN 'Diabetes - Very Poor Control'         -- ≥10.0%

        -- DCCT (%) categories
        WHEN measurement_type = 'DCCT' AND hba1c_value < 6.0 THEN 'Normal'
        WHEN measurement_type = 'DCCT' AND hba1c_value BETWEEN 6.0 AND 6.4 THEN 'Prediabetes'
        WHEN measurement_type = 'DCCT' AND hba1c_value BETWEEN 6.5 AND 6.9 THEN 'Diabetes - Target'
        WHEN measurement_type = 'DCCT' AND hba1c_value BETWEEN 7.0 AND 7.4 THEN 'Diabetes - Acceptable'
        WHEN measurement_type = 'DCCT' AND hba1c_value BETWEEN 7.5 AND 7.9 THEN 'Diabetes - Above Target'
        WHEN measurement_type = 'DCCT' AND hba1c_value BETWEEN 8.0 AND 9.9 THEN 'Diabetes - Poor Control'
        WHEN measurement_type = 'DCCT' AND hba1c_value >= 10.0 THEN 'Diabetes - Very Poor Control'

        ELSE 'Invalid'
    END AS hba1c_category,

    -- Diabetes diagnostic flag
    CASE
        WHEN (measurement_type = 'IFCC' AND hba1c_value >= 48) OR (measurement_type = 'DCCT' AND hba1c_value >= 6.5)
        THEN TRUE ELSE FALSE
    END AS indicates_diabetes,

    -- Target achievement flags for QOF
    CASE
        WHEN (measurement_type = 'IFCC' AND hba1c_value < 58) OR (measurement_type = 'DCCT' AND hba1c_value < 7.5)
        THEN TRUE ELSE FALSE
    END AS meets_qof_target

FROM base_observations

-- Sort for consistent output
ORDER BY person_id, clinical_effective_date DESC
