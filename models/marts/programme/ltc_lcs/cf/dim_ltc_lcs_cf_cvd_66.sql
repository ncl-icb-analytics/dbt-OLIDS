{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} SET COMMENT = 'CVD_66 case finding: Patients without cardiovascular risk factors (control group)'"
) }}

-- CVD_66 case finding: Statin review for patients aged 75-83
-- Identifies patients aged 75-83 who need QRISK2 assessment due to lack of recent readings

WITH base_population AS (
    -- Get base population aged 75-83
    SELECT
        bp.person_id,
        bp.age
    FROM {{ ref('int_ltc_lcs_cf_base_population') }} AS bp
    WHERE bp.age BETWEEN 75 AND 83
),

statin_medications AS (
    -- Get patients on any statins in last 12 months
    SELECT DISTINCT
        person_id,
        MAX(order_date) AS latest_statin_date
    FROM {{ ref('int_ltc_lcs_cvd_medications') }}
    WHERE
        cluster_id IN (
            'STATIN_CVD_MEDICATIONS',
            'STATIN_CVD_63_MEDICATIONS',
            'STATIN_CVD_64_MEDICATIONS',
            'STATIN_CVD_65_MEDICATIONS'
        )
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

health_checks AS (
    -- Get patients with health checks in last 24 months
    SELECT DISTINCT
        person_id,
        MAX(clinical_effective_date) AS latest_health_check_date
    FROM {{ ref('int_ltc_lcs_nhs_health_checks') }}
    WHERE clinical_effective_date >= DATEADD('month', -24, CURRENT_DATE())
    GROUP BY person_id
),

eligible_patients AS (
    -- Patients aged 75-83 not on statins, no allergies, no recent decisions, no health checks
    SELECT
        bp.person_id,
        bp.age
    FROM base_population AS bp
    LEFT JOIN statin_medications AS sm ON bp.person_id = sm.person_id
    LEFT JOIN statin_exclusions AS se USING (person_id)
    LEFT JOIN health_checks AS hc USING (person_id)
    WHERE
        NOT COALESCE(sm.person_id IS NOT NULL, FALSE)  -- Not on statins
        AND NOT COALESCE(se.latest_statin_allergy_date IS NOT NULL, FALSE)  -- No statin allergies
        AND NOT COALESCE(se.latest_statin_decision_date IS NOT NULL, FALSE)  -- No statin decisions
        AND NOT COALESCE(hc.person_id IS NOT NULL, FALSE)  -- No health checks in last 24 months
),

latest_qrisk2 AS (
-- Get latest QRISK2 readings for eligible patients (if any)
    SELECT
        obs.person_id,
        obs.clinical_effective_date AS latest_qrisk2_date,
        obs.result_value AS latest_qrisk2_value
    FROM {{ ref('int_ltc_lcs_cvd_observations') }} AS obs
    INNER JOIN eligible_patients ON obs.person_id = eligible_patients.person_id
    WHERE
        obs.cluster_id = 'QRISK2_10YEAR'
        AND obs.clinical_effective_date = (
            SELECT MAX(clinical_effective_date)
            FROM {{ ref('int_ltc_lcs_cvd_observations') }} AS obs2
            WHERE
                obs2.person_id = obs.person_id
                AND obs2.cluster_id = 'QRISK2_10YEAR'
        )
),

all_qrisk2_readings AS (
-- Get all QRISK2 readings for eligible patients
    SELECT
        obs.person_id,
        obs.clinical_effective_date,
        obs.result_value,
        obs.mapped_concept_code,
        obs.mapped_concept_display
    FROM {{ ref('int_ltc_lcs_cvd_observations') }} AS obs
    INNER JOIN eligible_patients ON obs.person_id = eligible_patients.person_id
    WHERE obs.cluster_id = 'QRISK2_10YEAR'
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

-- Final selection: patients aged 75-83 who need QRISK2 assessment
SELECT
    ep.person_id,
    ep.age,
    TRUE AS needs_qrisk2_assessment,  -- All patients in this cohort need QRISK2 assessment
    lq.latest_qrisk2_date,
    lq.latest_qrisk2_value,
    aqc.all_qrisk2_codes,
    aqc.all_qrisk2_displays
FROM eligible_patients AS ep
LEFT JOIN latest_qrisk2 AS lq ON ep.person_id = lq.person_id
LEFT JOIN all_qrisk2_codes AS aqc USING (person_id)
