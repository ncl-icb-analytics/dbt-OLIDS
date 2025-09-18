{{ config(
    materialized='table') }}
-- Intermediate model for LTC LCS Case Finding CVD_63
-- Identifies patients on statins (within last 6 months) with non-HDL cholesterol > 2.5 (statin review needed)

WITH statin_medications AS (
    -- Get all CVD_63 specific statin medications
    SELECT
        person_id,
        order_date,
        mapped_concept_code AS concept_code,
        mapped_concept_display AS concept_display
    FROM {{ ref('int_ltc_lcs_cvd_medications') }}
    WHERE cluster_id = 'LCS_STAT_COD_CVD'
        AND order_date >= dateadd(MONTH, -6, current_date())
),

latest_statin AS (
    -- Get the latest statin medication for each person
    SELECT
        person_id,
        order_date,
        concept_code,
        concept_display,
        ROW_NUMBER()
            OVER (PARTITION BY person_id ORDER BY order_date DESC)
            AS rn
    FROM statin_medications
    QUALIFY rn = 1
),

statin_codes AS (
    -- Aggregate all statin codes and displays for each person (within 6 months)
    SELECT
        person_id,
        ARRAY_AGG(DISTINCT concept_code) WITHIN GROUP (ORDER BY concept_code)
            AS all_statin_codes,
        ARRAY_AGG(DISTINCT concept_display) WITHIN GROUP (
            ORDER BY concept_display
        ) AS all_statin_displays
    FROM statin_medications
    GROUP BY person_id
),

non_hdl_readings AS (
    -- Get all non-HDL cholesterol readings with valid values
    SELECT
        person_id,
        clinical_effective_date,
        result_value,
        mapped_concept_code AS concept_code,
        mapped_concept_display AS concept_display
    FROM {{ ref('int_ltc_lcs_cvd_observations') }}
    WHERE
        cluster_id = 'NON_HDL_CHOLESTEROL'
        AND result_value IS NOT NULL
        AND CAST(result_value AS NUMBER) > 0
),

latest_non_hdl AS (
    -- Get the latest non-HDL reading for each person
    SELECT
        person_id,
        clinical_effective_date,
        result_value,
        concept_code,
        concept_display,
        ROW_NUMBER()
            OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC)
            AS rn
    FROM non_hdl_readings
    QUALIFY rn = 1
),

non_hdl_codes AS (
    -- Aggregate all non-HDL codes and displays for each person
    SELECT
        person_id,
        ARRAY_AGG(DISTINCT concept_code) WITHIN GROUP (ORDER BY concept_code)
            AS all_non_hdl_codes,
        ARRAY_AGG(DISTINCT concept_display) WITHIN GROUP (
            ORDER BY concept_display
        ) AS all_non_hdl_displays
    FROM non_hdl_readings
    GROUP BY person_id
)

-- Final selection
SELECT
    bp.person_id,
    bp.age,
    sm.order_date AS latest_statin_date,
    nh.clinical_effective_date AS latest_non_hdl_date,
    sc.all_statin_codes,
    sc.all_statin_displays,
    nhc.all_non_hdl_codes,
    nhc.all_non_hdl_displays,
    COALESCE(
        sm.person_id IS NOT NULL
        AND CAST(nh.result_value AS NUMBER) > 2.5,
        FALSE
    ) AS needs_statin_review,
    CAST(nh.result_value AS NUMBER) AS latest_non_hdl_value,
    COALESCE(
        sm.person_id IS NOT NULL
        AND CAST(nh.result_value AS NUMBER) > 2.5,
        FALSE
    ) AS meets_criteria
FROM {{ ref('int_ltc_lcs_cf_base_population') }} AS bp
INNER JOIN {{ ref('dim_person_age') }} AS age ON bp.person_id = age.person_id
LEFT JOIN latest_statin AS sm ON bp.person_id = sm.person_id
LEFT JOIN statin_codes AS sc ON bp.person_id = sc.person_id
LEFT JOIN latest_non_hdl AS nh ON bp.person_id = nh.person_id
LEFT JOIN non_hdl_codes AS nhc ON bp.person_id = nhc.person_id
WHERE
    age.age BETWEEN 40 AND 83  -- CVD age range
    AND sm.person_id IS NOT NULL  -- Must be on statins
    AND CAST(nh.result_value AS NUMBER) > 2.5  -- Must have elevated non-HDL cholesterol
