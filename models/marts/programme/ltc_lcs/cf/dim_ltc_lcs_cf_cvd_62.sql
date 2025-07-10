{{ config(
    materialized='table') }}
-- Intermediate model for LTC LCS Case Finding CVD_62
-- Identifies patients with QRISK2 score between 15-19.99% (case finding for cardiovascular disease prevention)

WITH qrisk2_readings AS (
    -- Get all QRISK2 readings with valid values
    SELECT
        person_id,
        clinical_effective_date,
        result_value,
        mapped_concept_code AS concept_code,
        mapped_concept_display AS concept_display
    FROM {{ ref('int_ltc_lcs_cvd_observations') }}
    WHERE
        cluster_id = 'QRISK2_10YEAR'
        AND result_value IS NOT NULL
        AND CAST(result_value AS NUMBER) > 0
),

latest_qrisk2 AS (
    -- Get the latest QRISK2 reading for each person
    SELECT
        person_id,
        clinical_effective_date,
        result_value,
        concept_code,
        concept_display,
        ROW_NUMBER()
            OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC)
            AS rn
    FROM qrisk2_readings
    QUALIFY rn = 1
),

qrisk2_codes AS (
    -- Aggregate all QRISK2 codes and displays for each person
    SELECT
        person_id,
        ARRAY_AGG(DISTINCT concept_code) WITHIN GROUP (ORDER BY concept_code)
            AS all_qrisk2_codes,
        ARRAY_AGG(DISTINCT concept_display) WITHIN GROUP (
            ORDER BY concept_display
        ) AS all_qrisk2_displays
    FROM qrisk2_readings
    GROUP BY person_id
)

-- Final selection
SELECT
    bp.person_id,
    bp.age,
    qr.clinical_effective_date AS latest_qrisk2_date,
    codes.all_qrisk2_codes,
    codes.all_qrisk2_displays,
    COALESCE(
        CAST(qr.result_value AS NUMBER) BETWEEN 15 AND 19.99,
        FALSE
    ) AS has_moderate_qrisk2,
    CAST(qr.result_value AS NUMBER) AS latest_qrisk2_value,
    COALESCE(
        CAST(qr.result_value AS NUMBER) BETWEEN 15 AND 19.99,
        FALSE
    ) AS meets_criteria
FROM {{ ref('int_ltc_lcs_cf_base_population') }} AS bp
INNER JOIN {{ ref('dim_person_age') }} AS age ON bp.person_id = age.person_id
LEFT JOIN latest_qrisk2 AS qr ON bp.person_id = qr.person_id
LEFT JOIN qrisk2_codes AS codes ON bp.person_id = codes.person_id
WHERE
    age.age BETWEEN 40 AND 83  -- CVD base population age range
    AND CAST(qr.result_value AS NUMBER) BETWEEN 15 AND 19.99  -- Only include patients who meet criteria
