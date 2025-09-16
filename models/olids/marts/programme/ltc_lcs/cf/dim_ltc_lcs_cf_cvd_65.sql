{{ config(
    materialized='table') }}

-- CVD_65 case finding: Moderate-dose statin case finding
-- Identifies patients with QRISK2 ≥ 10 who need moderate-dose statins

WITH qrisk2_patients AS (
    -- Get patients with latest QRISK2 ≥ 10
    SELECT DISTINCT
        obs.person_id,
        bp.age,
        obs.clinical_effective_date AS latest_qrisk2_date,
        obs.result_value AS latest_qrisk2_value
    FROM {{ ref('int_ltc_lcs_cvd_observations') }} AS obs
    INNER JOIN
        {{ ref('int_ltc_lcs_cf_base_population') }} AS bp
        ON obs.person_id = bp.person_id
    WHERE
        obs.cluster_id = 'QRISK2_10YEAR'
        AND obs.result_value >= 10
        AND obs.clinical_effective_date = (
            SELECT MAX(clinical_effective_date)
            FROM {{ ref('int_ltc_lcs_cvd_observations') }} AS obs2
            WHERE
                obs2.person_id = obs.person_id
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
    WHERE
        cluster_id = 'LCS_STAT_COD_CVD'
        AND order_date >= DATEADD('month', -12, CURRENT_DATE())
    GROUP BY person_id
),

statin_exclusions AS (
-- Get patients with statin allergies/contraindications or recent decisions
    SELECT DISTINCT
        person_id,
        MAX(CASE
            WHEN
                cluster_id IN (
                    'STATIN_ALLERGY_ADVERSE_REACTION', 'STATIN_NOT_INDICATED'
                )
                THEN clinical_effective_date
        END) AS latest_statin_allergy_date,
        MAX(CASE
            WHEN cluster_id = 'STATINDEC_COD'
                THEN clinical_effective_date
        END) AS latest_statin_decision_date
    FROM {{ ref('int_ltc_lcs_cvd_observations') }}
    WHERE
        cluster_id IN (
            'STATIN_ALLERGY_ADVERSE_REACTION',
            'STATIN_NOT_INDICATED',
            'STATINDEC_COD'
        )
        AND (
            (
                cluster_id IN (
                    'STATIN_ALLERGY_ADVERSE_REACTION', 'STATIN_NOT_INDICATED'
                )
            )
            OR (
                cluster_id = 'STATINDEC_COD'
                AND clinical_effective_date
                >= DATEADD('month', -60, CURRENT_DATE())
            )
        )
    GROUP BY person_id
),

eligible_patients AS (
-- QRISK2 ≥ 10 patients not on moderate-dose statins, no allergies, no recent decisions
    SELECT
        qp.person_id,
        qp.age,
        qp.latest_qrisk2_date,
        qp.latest_qrisk2_value
    FROM qrisk2_patients AS qp
    LEFT JOIN moderate_dose_statins AS mds ON qp.person_id = mds.person_id
    LEFT JOIN statin_exclusions AS se ON qp.person_id = se.person_id
    WHERE
        NOT COALESCE(mds.person_id IS NOT NULL, FALSE)  -- Not on moderate-dose statins
        AND NOT COALESCE(se.latest_statin_allergy_date IS NOT NULL, FALSE)  -- No statin allergies
        AND NOT COALESCE(se.latest_statin_decision_date IS NOT NULL, FALSE)  -- No statin decisions
),

all_qrisk2_readings AS (
-- Get all QRISK2 readings ≥ 10 for eligible patients
    SELECT
        obs.person_id,
        obs.clinical_effective_date,
        obs.result_value,
        obs.mapped_concept_code,
        obs.mapped_concept_display
    FROM {{ ref('int_ltc_lcs_cvd_observations') }} AS obs
    INNER JOIN eligible_patients ON obs.person_id = eligible_patients.person_id
    WHERE
        obs.cluster_id = 'QRISK2_10YEAR'
        AND obs.result_value >= 10
),

all_qrisk2_codes AS (
-- Aggregate all QRISK2 codes and displays for each person
    SELECT
        person_id,
        ARRAY_AGG(DISTINCT mapped_concept_code) WITHIN GROUP (
            ORDER BY mapped_concept_code
        ) AS all_qrisk2_codes,
        ARRAY_AGG(DISTINCT mapped_concept_display) WITHIN GROUP (
            ORDER BY mapped_concept_display
        ) AS all_qrisk2_displays
    FROM all_qrisk2_readings
    GROUP BY person_id
)

-- Final selection: patients who need moderate-dose statins (ensure one row per person)
SELECT
    ep.person_id,
    ep.age,
    TRUE AS needs_moderate_dose_statin,  -- All patients in this cohort need moderate-dose statins
    ep.latest_qrisk2_date,
    ep.latest_qrisk2_value,
    aqc.all_qrisk2_codes,
    aqc.all_qrisk2_displays
FROM eligible_patients AS ep
LEFT JOIN all_qrisk2_codes AS aqc ON ep.person_id = aqc.person_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY ep.person_id ORDER BY ep.latest_qrisk2_date DESC, ep.latest_qrisk2_value DESC) = 1
