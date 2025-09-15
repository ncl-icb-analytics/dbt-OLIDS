{{ config(
    materialized='table') }}

-- Intermediate model for LTC LCS CF DM_62 case finding
-- Patients with gestational diabetes and pregnancy risk who meet ALL of the following criteria:
-- 1. Has gestational diabetes and pregnancy risk diagnosis
-- 2. No HbA1c reading in the last 12 months

WITH base_population AS (
    -- Get base population aged 17+ (already excludes LTC registers and NHS health checks)
    SELECT DISTINCT
        person_id,
        age
    FROM {{ ref('int_ltc_lcs_cf_base_population') }}
    WHERE age >= 17
),

gestational_diabetes_risk AS (
    -- Get patients with gestational diabetes and pregnancy risk
    SELECT DISTINCT
        person_id,
        ARRAY_AGG(DISTINCT mapped_concept_code) WITHIN GROUP (
            ORDER BY mapped_concept_code
        ) AS all_gestational_diabetes_codes,
        ARRAY_AGG(DISTINCT mapped_concept_display) WITHIN GROUP (
            ORDER BY mapped_concept_display
        ) AS all_gestational_diabetes_displays
    FROM {{ ref('int_ltc_lcs_dm_observations') }}
    WHERE cluster_id = 'DM_GESTDIAB_AND_PREG_RISK'
    GROUP BY person_id
),

latest_hba1c AS (
    -- Get the most recent HbA1c reading for each person
    SELECT
        person_id,
        clinical_effective_date AS latest_hba1c_date,
        result_value AS latest_hba1c_value
    FROM {{ ref('int_ltc_lcs_dm_observations') }}
    WHERE
        cluster_id = 'HBA1C_LEVEL'
        AND result_value > 0
    QUALIFY
        ROW_NUMBER()
            OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC)
        = 1
)

-- Final selection with gestational diabetes risk assessment
SELECT
    bp.person_id,
    bp.age,
    hba1c.latest_hba1c_date,
    hba1c.latest_hba1c_value,
    gd.all_gestational_diabetes_codes,
    gd.all_gestational_diabetes_displays,
    COALESCE(
        gd.person_id IS NOT NULL
        AND (
            hba1c.latest_hba1c_date IS NULL
            OR hba1c.latest_hba1c_date < DATEADD(YEAR, -1, CURRENT_DATE())
        ), FALSE
    ) AS has_gestational_diabetes_risk
FROM base_population AS bp
LEFT JOIN gestational_diabetes_risk AS gd ON bp.person_id = gd.person_id
LEFT JOIN latest_hba1c AS hba1c ON bp.person_id = hba1c.person_id
WHERE
    gd.person_id IS NOT NULL
    AND (
        hba1c.latest_hba1c_date IS NULL
        OR hba1c.latest_hba1c_date < DATEADD(YEAR, -1, CURRENT_DATE())
    )
