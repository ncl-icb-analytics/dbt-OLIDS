{{ config(materialized='ephemeral') }}

-- Intermediate model for LTC LCS CF DM_63 case finding
-- Patients who meet ALL of the following criteria:
-- 1. Latest HbA1c reading is ≥ 46 but < 48 mmol/mol
-- 2. No HbA1c reading in the last 12 months

WITH base_population AS (
    -- Get base population aged 17+ (already excludes LTC registers and NHS health checks)
    SELECT DISTINCT
        person_id,
        age
    FROM {{ ref('int_ltc_lcs_cf_base_population') }}
    WHERE age >= 17
),

hba1c_readings AS (
    -- Get all HbA1c readings with values > 0
    SELECT
        person_id,
        clinical_effective_date,
        result_value,
        mapped_concept_code,
        mapped_concept_display
    FROM {{ ref('int_ltc_lcs_dm_observations') }}
    WHERE cluster_id = 'HBA1C_LEVEL'
        AND result_value > 0
),

latest_hba1c AS (
    -- Get the most recent HbA1c reading for each person
    SELECT
        person_id,
        clinical_effective_date AS latest_hba1c_date,
        result_value AS latest_hba1c_value,
        ARRAY_AGG(DISTINCT mapped_concept_code) WITHIN GROUP (ORDER BY mapped_concept_code) AS all_hba1c_codes,
        ARRAY_AGG(DISTINCT mapped_concept_display) WITHIN GROUP (ORDER BY mapped_concept_display) AS all_hba1c_displays
    FROM hba1c_readings
    GROUP BY person_id, clinical_effective_date, result_value
    QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC) = 1
),

recent_hba1c AS (
    -- Get patients with HbA1c in last 12 months (for exclusion)
    SELECT DISTINCT
        person_id
    FROM hba1c_readings
    WHERE clinical_effective_date >= DATEADD(year, -1, CURRENT_DATE())
)

-- Final selection with elevated HbA1c assessment
SELECT
    bp.person_id,
    bp.age,
    CASE 
        WHEN hba1c.latest_hba1c_value >= 46 
             AND hba1c.latest_hba1c_value < 48 
             AND rh.person_id IS NULL THEN TRUE
        ELSE FALSE
    END AS has_elevated_hba1c,
    hba1c.latest_hba1c_date,
    hba1c.latest_hba1c_value,
    hba1c.all_hba1c_codes,
    hba1c.all_hba1c_displays
FROM base_population bp
LEFT JOIN latest_hba1c hba1c ON bp.person_id = hba1c.person_id
LEFT JOIN recent_hba1c rh ON bp.person_id = rh.person_id
WHERE hba1c.latest_hba1c_value >= 46 
    AND hba1c.latest_hba1c_value < 48
    AND rh.person_id IS NULL 