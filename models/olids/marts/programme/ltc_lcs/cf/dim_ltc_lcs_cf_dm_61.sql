{{ config(
    materialized='table') }}

-- Intermediate model for LTC LCS CF DM_61 case finding
-- Patients at risk of diabetes who meet ANY of the following criteria:
-- 1. HbA1c ≥ 42 mmol/mol within the last 5 years
-- 2. QDiabetes score ≥ 5.6%
-- 3. QRisk2 score > 20%
-- 4. History of gestational diabetes

WITH base_population AS (
    -- Get base population aged 17+ (already excludes LTC registers and NHS health checks)
    SELECT DISTINCT
        person_id,
        age
    FROM {{ ref('int_ltc_lcs_cf_base_population') }}
    WHERE age >= 17
),

hba1c_readings AS (
    -- Get all HbA1c readings with values > 0 within last 5 years
    SELECT
        person_id,
        clinical_effective_date,
        result_value,
        mapped_concept_code,
        mapped_concept_display
    FROM {{ ref('int_ltc_lcs_dm_observations') }}
    WHERE
        cluster_id = 'HBA1C'
        AND result_value > 0
        AND clinical_effective_date >= DATEADD(YEAR, -5, CURRENT_DATE())
),

latest_hba1c AS (
    -- Get the most recent HbA1c reading for each person
    SELECT
        person_id,
        clinical_effective_date AS latest_hba1c_date,
        result_value AS latest_hba1c_value,
        ARRAY_AGG(DISTINCT mapped_concept_code) WITHIN GROUP (
            ORDER BY mapped_concept_code
        ) AS all_hba1c_codes,
        ARRAY_AGG(DISTINCT mapped_concept_display) WITHIN GROUP (
            ORDER BY mapped_concept_display
        ) AS all_hba1c_displays
    FROM hba1c_readings
    GROUP BY person_id, clinical_effective_date, result_value
    QUALIFY
        ROW_NUMBER()
            OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC)
        = 1
),

latest_qdiabetes AS (
    -- Get the most recent QDiabetes score for each person
    SELECT
        person_id,
        clinical_effective_date AS latest_qdiabetes_date,
        result_value AS latest_qdiabetes_value
    FROM {{ ref('int_ltc_lcs_dm_observations') }}
    WHERE cluster_id = 'QDIABETES_RISK'
    QUALIFY
        ROW_NUMBER()
            OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC)
        = 1
),

latest_qrisk AS (
    -- Get the most recent QRisk2 score for each person
    SELECT
        person_id,
        clinical_effective_date AS latest_qrisk_date,
        result_value AS latest_qrisk_value
    FROM {{ ref('int_ltc_lcs_dm_observations') }}
    WHERE cluster_id = 'QRISK2_10YEAR'
    QUALIFY
        ROW_NUMBER()
            OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC)
        = 1
),

gestational_diabetes AS (
    -- Get patients with history of gestational diabetes
    SELECT DISTINCT
        person_id,
        TRUE AS has_gestational_diabetes
    FROM {{ ref('int_ltc_lcs_dm_observations') }}
    WHERE cluster_id = 'GESTDIAB_COD'
)

-- Final selection with risk assessment
SELECT
    bp.person_id,
    bp.age,
    hba1c.latest_hba1c_date,
    hba1c.latest_hba1c_value,
    qd.latest_qdiabetes_date,
    qd.latest_qdiabetes_value,
    qr.latest_qrisk_date,
    qr.latest_qrisk_value,
    hba1c.all_hba1c_codes,
    hba1c.all_hba1c_displays,
    COALESCE(
        hba1c.latest_hba1c_value >= 42
        OR qd.latest_qdiabetes_value >= 5.6
        OR qr.latest_qrisk_value > 20
        OR gd.has_gestational_diabetes = TRUE, FALSE
    ) AS has_diabetes_risk,
    COALESCE(gd.has_gestational_diabetes, FALSE) AS has_gestational_diabetes
FROM base_population AS bp
LEFT JOIN latest_hba1c AS hba1c ON bp.person_id = hba1c.person_id
LEFT JOIN latest_qdiabetes AS qd ON bp.person_id = qd.person_id
LEFT JOIN latest_qrisk AS qr ON bp.person_id = qr.person_id
LEFT JOIN gestational_diabetes AS gd ON bp.person_id = gd.person_id
WHERE
    hba1c.latest_hba1c_value >= 42
    OR qd.latest_qdiabetes_value >= 5.6
    OR qr.latest_qrisk_value > 20
    OR gd.has_gestational_diabetes = TRUE
