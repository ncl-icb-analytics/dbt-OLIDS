{{ config(
    materialized='table') }}

-- Intermediate model for LTC LCS CF DM_64 case finding
-- Patients who meet ALL of the following criteria:
-- 1. High BMI based on ethnicity (BMI ≥ 32.5 for BAME, ≥ 35 for non-BAME)
-- 2. No HbA1c reading in the last 24 months

WITH base_population AS (
    -- Get base population aged 17+ (already excludes LTC registers and NHS health checks)
    SELECT DISTINCT
        person_id,
        age
    FROM {{ ref('int_ltc_lcs_cf_base_population') }}
    WHERE age >= 17
),

bame_population AS (
    -- Get patients from BAME ethnicity (excluding White British and excluded ethnicities)
    SELECT DISTINCT
        person_id,
        TRUE AS is_bame
    FROM {{ ref('int_ltc_lcs_ethnicity_observations') }}
    WHERE cluster_id = 'BAME_ETHNICITY'
    EXCEPT
    SELECT DISTINCT
        person_id,
        TRUE AS is_bame
    FROM {{ ref('int_ltc_lcs_ethnicity_observations') }}
    WHERE
        cluster_id IN ('WHITE_BRITISH', 'DM_EXCL_ETHNICITY')
),

bmi_measurements AS (
    -- Get all BMI measurements with values > 0
    SELECT
        person_id,
        clinical_effective_date,
        result_value,
        mapped_concept_code,
        mapped_concept_display
    FROM {{ ref('int_ltc_lcs_dm_observations') }}
    WHERE
        cluster_id = 'BMI_CODES'
        AND result_value > 0
),

latest_bmi AS (
    -- Get the most recent BMI measurement for each person
    SELECT
        person_id,
        clinical_effective_date AS latest_bmi_date,
        result_value AS latest_bmi_value,
        ARRAY_AGG(DISTINCT mapped_concept_code) WITHIN GROUP (
            ORDER BY mapped_concept_code
        ) AS all_bmi_codes,
        ARRAY_AGG(DISTINCT mapped_concept_display) WITHIN GROUP (
            ORDER BY mapped_concept_display
        ) AS all_bmi_displays
    FROM bmi_measurements
    GROUP BY person_id, clinical_effective_date, result_value
    QUALIFY
        ROW_NUMBER()
            OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC)
        = 1
),

recent_hba1c AS (
    -- Get patients with HbA1c in last 24 months (for exclusion)
    SELECT DISTINCT
        person_id,
        clinical_effective_date AS latest_hba1c_date,
        result_value AS latest_hba1c_value
    FROM {{ ref('int_ltc_lcs_dm_observations') }}
    WHERE
        cluster_id = 'HBA1C_LEVEL'
        AND result_value > 0
        AND clinical_effective_date >= DATEADD(YEAR, -2, CURRENT_DATE())
    QUALIFY
        ROW_NUMBER()
            OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC)
        = 1
)

-- Final selection with ethnicity-based BMI assessment
SELECT
    bp.person_id,
    bp.age,
    bmi.latest_bmi_date,
    bmi.latest_bmi_value,
    hba1c.latest_hba1c_date,
    hba1c.latest_hba1c_value,
    bmi.all_bmi_codes,
    bmi.all_bmi_displays,
    COALESCE(
        (bame.is_bame = TRUE AND bmi.latest_bmi_value >= 32.5)
        OR (bame.is_bame IS NULL AND bmi.latest_bmi_value >= 35), FALSE
    ) AS has_high_bmi,
    COALESCE(bame.is_bame, FALSE) AS is_bame
FROM base_population AS bp
LEFT JOIN bame_population AS bame ON bp.person_id = bame.person_id
LEFT JOIN latest_bmi AS bmi ON bp.person_id = bmi.person_id
LEFT JOIN recent_hba1c AS hba1c ON bp.person_id = hba1c.person_id
WHERE (
    (bame.is_bame = TRUE AND bmi.latest_bmi_value >= 32.5)
    OR (bame.is_bame IS NULL AND bmi.latest_bmi_value >= 35)
)
AND hba1c.person_id IS NULL
