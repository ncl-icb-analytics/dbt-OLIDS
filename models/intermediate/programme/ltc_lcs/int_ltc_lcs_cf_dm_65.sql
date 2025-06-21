{{ config(materialized='ephemeral') }}

-- Intermediate model for LTC LCS CF DM_65 case finding
-- Patients who meet ALL of the following criteria:
-- 1. Moderate-high BMI based on ethnicity (BMI 27.5-32.5 for BAME, 30-35 for non-BAME)
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
    WHERE cluster_id = 'ETHNICITY_BAME'
    EXCEPT
    SELECT DISTINCT
        person_id,
        TRUE AS is_bame
    FROM {{ ref('int_ltc_lcs_ethnicity_observations') }}
    WHERE cluster_id IN ('ETHNICITY_WHITE_BRITISH', 'DIABETES_EXCLUDED_ETHNICITY')
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
    WHERE cluster_id = 'BMI_MEASUREMENT'
        AND result_value > 0
),

latest_bmi AS (
    -- Get the most recent BMI measurement for each person
    SELECT
        person_id,
        clinical_effective_date AS latest_bmi_date,
        result_value AS latest_bmi_value,
        ARRAY_AGG(DISTINCT mapped_concept_code) WITHIN GROUP (ORDER BY mapped_concept_code) AS all_bmi_codes,
        ARRAY_AGG(DISTINCT mapped_concept_display) WITHIN GROUP (ORDER BY mapped_concept_display) AS all_bmi_displays
    FROM bmi_measurements
    GROUP BY person_id, clinical_effective_date, result_value
    QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC) = 1
),

recent_hba1c AS (
    -- Get patients with HbA1c in last 24 months (for exclusion)
    SELECT DISTINCT
        person_id,
        clinical_effective_date AS latest_hba1c_date,
        result_value AS latest_hba1c_value
    FROM {{ ref('int_ltc_lcs_dm_observations') }}
    WHERE cluster_id = 'HBA1C_LEVEL'
        AND result_value > 0
        AND clinical_effective_date >= DATEADD(year, -2, CURRENT_DATE())
    QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC) = 1
)

-- Final selection with moderate BMI assessment
SELECT
    bp.person_id,
    bp.age,
    CASE 
        WHEN (bame.is_bame = TRUE AND bmi.latest_bmi_value >= 27.5 AND bmi.latest_bmi_value < 32.5) OR
             (bame.is_bame IS NULL AND bmi.latest_bmi_value >= 30 AND bmi.latest_bmi_value < 35) THEN TRUE
        ELSE FALSE
    END AS has_moderate_high_bmi,
    COALESCE(bame.is_bame, FALSE) AS is_bame,
    bmi.latest_bmi_date,
    bmi.latest_bmi_value,
    hba1c.latest_hba1c_date,
    hba1c.latest_hba1c_value,
    bmi.all_bmi_codes,
    bmi.all_bmi_displays
FROM base_population bp
LEFT JOIN bame_population bame ON bp.person_id = bame.person_id
LEFT JOIN latest_bmi bmi ON bp.person_id = bmi.person_id
LEFT JOIN recent_hba1c hba1c ON bp.person_id = hba1c.person_id
WHERE ((bame.is_bame = TRUE AND bmi.latest_bmi_value >= 27.5 AND bmi.latest_bmi_value < 32.5) OR
       (bame.is_bame IS NULL AND bmi.latest_bmi_value >= 30 AND bmi.latest_bmi_value < 35))
    AND hba1c.person_id IS NULL 