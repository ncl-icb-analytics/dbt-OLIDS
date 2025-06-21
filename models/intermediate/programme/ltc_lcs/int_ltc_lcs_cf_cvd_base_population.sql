{{ config(materialized='ephemeral') }}

-- General CVD base population for case finding indicators
-- Includes patients aged 40-83 who are not on statins, have no statin allergies/contraindications, and no recent statin decisions

WITH base_population AS (
    -- Get base population aged 40-83
    SELECT 
        bp.person_id,
        bp.age
    FROM {{ ref('int_ltc_lcs_cf_base_population') }} bp
    WHERE bp.age BETWEEN 40 AND 83
),

statin_medications AS (
    -- Get patients on any statins in last 12 months
    SELECT DISTINCT
        person_id,
        MAX(order_date) AS latest_statin_date
    FROM {{ ref('int_ltc_lcs_cvd_medications') }}
    WHERE cluster_id IN ('STATIN_CVD_MEDICATIONS', 'STATIN_CVD_63_MEDICATIONS', 'STATIN_CVD_64_MEDICATIONS', 'STATIN_CVD_65_MEDICATIONS')
        AND order_date >= DATEADD('month', -12, CURRENT_DATE())
    GROUP BY person_id
),

statin_exclusions AS (
    -- Get patients with statin allergies/contraindications or recent decisions
    SELECT DISTINCT
        person_id,
        MAX(CASE WHEN cluster_id IN ('STATIN_ALLERGY_ADVERSE_REACTION', 'STATIN_NOT_INDICATED') 
                 THEN clinical_effective_date END) AS latest_statin_allergy_date,
        MAX(CASE WHEN cluster_id = 'STATINDEC_COD' 
                 THEN clinical_effective_date END) AS latest_statin_decision_date
    FROM {{ ref('int_ltc_lcs_cvd_observations') }}
    WHERE cluster_id IN ('STATIN_ALLERGY_ADVERSE_REACTION', 'STATIN_NOT_INDICATED', 'STATINDEC_COD')
        AND (
            (cluster_id IN ('STATIN_ALLERGY_ADVERSE_REACTION', 'STATIN_NOT_INDICATED'))
            OR (cluster_id = 'STATINDEC_COD' AND clinical_effective_date >= DATEADD('month', -60, CURRENT_DATE()))
        )
    GROUP BY person_id
)

-- Final selection: patients not on statins, no allergies, no recent decisions
SELECT
    bp.person_id,
    bp.age,
    COALESCE(sm.person_id IS NOT NULL, FALSE) AS has_statin,
    COALESCE(se.latest_statin_allergy_date IS NOT NULL, FALSE) AS has_statin_allergy,
    COALESCE(se.latest_statin_decision_date IS NOT NULL, FALSE) AS has_statin_decision,
    sm.latest_statin_date,
    se.latest_statin_allergy_date,
    se.latest_statin_decision_date
FROM base_population bp
LEFT JOIN statin_medications sm USING (person_id)
LEFT JOIN statin_exclusions se USING (person_id)
WHERE NOT COALESCE(sm.person_id IS NOT NULL, FALSE)  -- Not on statins
    AND NOT COALESCE(se.latest_statin_allergy_date IS NOT NULL, FALSE)  -- No statin allergies
    AND NOT COALESCE(se.latest_statin_decision_date IS NOT NULL, FALSE)  -- No statin decisions 