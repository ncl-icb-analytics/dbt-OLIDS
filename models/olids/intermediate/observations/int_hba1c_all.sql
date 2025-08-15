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

,

standardised AS (
    /* Convert all values to IFCC (mmol/mol) using NGSP/DCCT ↔ IFCC relationship
       Formulae (IFCC mmol/mol ↔ NGSP %):
       NGSP% = 0.09148 * IFCC + 2.152
       IFCC  = 10.929 * NGSP% - 23.5
    */
    SELECT
        bo.*,
        CASE
            WHEN bo.measurement_type = 'IFCC' THEN bo.hba1c_value
            WHEN bo.measurement_type = 'DCCT' THEN ROUND(10.929 * bo.hba1c_value - 23.5, 0)
            ELSE NULL
        END AS hba1c_value_ifcc_standardised,
        CASE
            WHEN bo.measurement_type = 'DCCT' THEN bo.hba1c_value
            WHEN bo.measurement_type = 'IFCC' THEN ROUND((0.09148 * bo.hba1c_value) + 2.152, 1)
            ELSE NULL
        END AS hba1c_dcct_value
    FROM base_observations bo
)

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    hba1c_value,
    hba1c_value_ifcc_standardised,
    hba1c_dcct_value,
    /* Canonicalise units by measurement type: IFCC → mmol/mol, DCCT → %.
       Override NULL/blank/UNKNOWN/HbA1c or mismatched units. */
    CASE
        WHEN measurement_type = 'IFCC' THEN 'mmol/mol'
        WHEN measurement_type = 'DCCT' THEN '%'
        ELSE COALESCE(NULLIF(TRIM(result_unit_display), ''), 'UNKNOWN')
    END AS result_unit_display,
    concept_code,
    concept_display,
    source_cluster_id,
    measurement_type,
    (measurement_type = 'IFCC') AS is_ifcc,
    (measurement_type = 'DCCT') AS is_dcct,
    original_result_value,

    -- Use actual result unit display from source data (enhanced macro)

    -- Enhanced result string for reporting (value + unit from source)
    CAST(hba1c_value AS VARCHAR) || ' ' ||
        CASE
            WHEN measurement_type = 'IFCC' THEN 'mmol/mol'
            WHEN measurement_type = 'DCCT' THEN '%'
            ELSE COALESCE(NULLIF(TRIM(result_unit_display), ''), 'UNKNOWN')
        END AS hba1c_result_display,

    -- Data quality validation (enhanced range checks)
    CASE
        WHEN measurement_type = 'IFCC' AND hba1c_value BETWEEN 20 AND 200 THEN TRUE  -- IFCC: mmol/mol (expanded range)
        WHEN measurement_type = 'DCCT' AND hba1c_value BETWEEN 3 AND 20 THEN TRUE    -- DCCT: %
        ELSE FALSE
    END AS is_valid_hba1c,

    -- NICE-aligned clinical categorisation using standardised IFCC value
    CASE
        WHEN hba1c_value_ifcc_standardised IS NULL THEN 'Invalid'
        WHEN hba1c_value_ifcc_standardised < 42 THEN 'Normal'                -- < 6.0%
        WHEN hba1c_value_ifcc_standardised >= 42 AND hba1c_value_ifcc_standardised < 48 THEN 'Prediabetes' -- 42.0–47.9
        WHEN hba1c_value_ifcc_standardised >= 48 AND hba1c_value_ifcc_standardised < 54 THEN 'Diabetes - At NICE Target' -- 48.0–53.9
        WHEN hba1c_value_ifcc_standardised >= 54 AND hba1c_value_ifcc_standardised < 58 THEN 'Diabetes - Acceptable (within QOF)' -- 54.0–57.9
        WHEN hba1c_value_ifcc_standardised >= 58 AND hba1c_value_ifcc_standardised < 75 THEN 'Diabetes - Above Target' -- 58.0–74.9
        WHEN hba1c_value_ifcc_standardised >= 75 AND hba1c_value_ifcc_standardised < 86 THEN 'Diabetes - High Risk' -- 75.0–85.9
        WHEN hba1c_value_ifcc_standardised >= 86 THEN 'Diabetes - Very High Risk'
        ELSE 'Invalid'
    END AS hba1c_category,

    -- Diabetes diagnostic flag
    CASE
        WHEN hba1c_value_ifcc_standardised >= 48 THEN TRUE ELSE FALSE
    END AS indicates_diabetes,

    -- Target achievement flags for QOF
    CASE
        WHEN hba1c_value_ifcc_standardised < 58 THEN TRUE ELSE FALSE
    END AS meets_qof_target

FROM standardised

-- Sort for consistent output
ORDER BY person_id, clinical_effective_date DESC
