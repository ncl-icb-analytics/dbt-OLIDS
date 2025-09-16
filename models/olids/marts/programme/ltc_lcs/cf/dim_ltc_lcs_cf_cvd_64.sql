{{ config(
    materialized='table') }}

-- CVD_64 case finding: High-dose statin case finding
-- Identifies patients from CVD base population who are NOT on statins (need to start)
-- Excludes those with recent statin medications (12 months) or statin decisions (60 months)

WITH statin_medications AS (
    -- Get patients with statin medications in last 12 months
    SELECT DISTINCT
        person_id,
        MAX(order_date) AS latest_statin_date
    FROM {{ ref('int_ltc_lcs_cvd_medications') }}
    WHERE 
        cluster_id = 'LCS_STAT_COD_CVD'
        AND order_date >= dateadd(MONTH, -12, current_date())
    GROUP BY person_id
),

statin_decisions AS (
    -- Get patients with statin decisions in last 60 months
    SELECT DISTINCT
        person_id,
        MAX(clinical_effective_date) AS latest_decision_date
    FROM {{ ref('int_ltc_lcs_cvd_observations') }}
    WHERE 
        cluster_id = 'STATINDEC_COD'
        AND clinical_effective_date >= dateadd(MONTH, -60, current_date())
    GROUP BY person_id
)

-- Final selection: CVD base population EXCLUDING those on statins or with statin decisions
SELECT
    bp.person_id,
    bp.age,
    TRUE AS needs_high_dose_statin,
    sm.latest_statin_date,
    sd.latest_decision_date,
    CASE 
        WHEN sm.person_id IS NOT NULL THEN 'On statins (last 12 months)'
        WHEN sd.person_id IS NOT NULL THEN 'Statin decision (last 60 months)'
        ELSE NULL
    END AS exclusion_reason
FROM {{ ref('int_ltc_lcs_cf_cvd_base_population') }} AS bp
LEFT JOIN statin_medications AS sm ON bp.person_id = sm.person_id
LEFT JOIN statin_decisions AS sd ON bp.person_id = sd.person_id
WHERE 
    sm.person_id IS NULL  -- Not on statins in last 12 months
    AND sd.person_id IS NULL  -- No statin decision in last 60 months