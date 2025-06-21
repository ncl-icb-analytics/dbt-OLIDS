{{ config(materialized='ephemeral') }}

-- CVD_65 base population for moderate-dose statin case finding
-- Includes patients with QRISK2 ≥ 10 who are not on moderate-dose statins, have no statin allergies/contraindications, and no recent statin decisions

WITH qrisk2_patients AS (
    -- Get patients with latest QRISK2 ≥ 10
    SELECT 
        obs.person_id,
        bp.age,
        obs.clinical_effective_date AS latest_qrisk2_date,
        obs.result_value AS latest_qrisk2_value
    FROM {{ ref('int_ltc_lcs_cvd_observations') }} obs
    JOIN {{ ref('int_ltc_lcs_cf_base_population') }} bp USING (person_id)
    WHERE obs.cluster_id = 'QRISK2_10YEAR'
        AND obs.result_value >= 10
        AND obs.clinical_effective_date = (
            SELECT MAX(clinical_effective_date)
            FROM {{ ref('int_ltc_lcs_cvd_observations') }} obs2
            WHERE obs2.person_id = obs.person_id
                AND obs2.cluster_id = 'QRISK2_10YEAR'
                AND obs2.result_value >= 10
        )
),

moderate_dose_statins AS (
    -- Get patients on moderate-dose statins in last 12 months
    SELECT DISTINCT
        person_id,
        MAX(order_date) AS latest_moderate_dose_statin_date
    FROM {{ ref('int_ltc_lcs_cvd_medications') }}
    WHERE cluster_id = 'STATIN_CVD_65_MEDICATIONS'
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

-- Final selection: QRISK2 ≥ 10 patients not on moderate-dose statins, no allergies, no recent decisions
SELECT
    qp.person_id,
    qp.age,
    qp.latest_qrisk2_date,
    qp.latest_qrisk2_value,
    COALESCE(mds.person_id IS NOT NULL, FALSE) AS has_moderate_dose_statin,
    COALESCE(se.latest_statin_allergy_date IS NOT NULL, FALSE) AS has_statin_allergy,
    COALESCE(se.latest_statin_decision_date IS NOT NULL, FALSE) AS has_statin_decision,
    mds.latest_moderate_dose_statin_date,
    se.latest_statin_allergy_date,
    se.latest_statin_decision_date
FROM qrisk2_patients qp
LEFT JOIN moderate_dose_statins mds USING (person_id)
LEFT JOIN statin_exclusions se USING (person_id)
WHERE NOT COALESCE(mds.person_id IS NOT NULL, FALSE)  -- Not on moderate-dose statins
    AND NOT COALESCE(se.latest_statin_allergy_date IS NOT NULL, FALSE)  -- No statin allergies
    AND NOT COALESCE(se.latest_statin_decision_date IS NOT NULL, FALSE)  -- No statin decisions 