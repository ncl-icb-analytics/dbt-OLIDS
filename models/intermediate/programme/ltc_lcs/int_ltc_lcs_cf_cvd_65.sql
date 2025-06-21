{{ config(materialized='ephemeral') }}

-- CVD_65 case finding: Moderate-dose statin case finding
-- Identifies patients with QRISK2 ≥ 10 who need moderate-dose statins

WITH all_qrisk2_readings AS (
    -- Get all QRISK2 readings ≥ 10 for patients in base
    SELECT
        obs.person_id,
        obs.clinical_effective_date,
        obs.result_value,
        obs.mapped_concept_code,
        obs.mapped_concept_display
    FROM {{ ref('int_ltc_lcs_cvd_observations') }} obs
    JOIN {{ ref('int_ltc_lcs_cf_cvd_65_base_population') }} bp USING (person_id)
    WHERE obs.cluster_id = 'QRISK2_10YEAR'
        AND obs.result_value >= 10
),

all_qrisk2_codes AS (
    -- Aggregate all QRISK2 codes and displays for each person
    SELECT
        person_id,
        ARRAY_AGG(DISTINCT mapped_concept_code) WITHIN GROUP (ORDER BY mapped_concept_code) AS all_qrisk2_codes,
        ARRAY_AGG(DISTINCT mapped_concept_display) WITHIN GROUP (ORDER BY mapped_concept_display) AS all_qrisk2_displays
    FROM all_qrisk2_readings
    GROUP BY person_id
)

-- Final selection: patients from CVD_65 base who need moderate-dose statins
SELECT
    bp.person_id,
    bp.age,
    TRUE AS needs_moderate_dose_statin,  -- All patients in base need moderate-dose statins
    bp.latest_qrisk2_date,
    bp.latest_qrisk2_value,
    aqc.all_qrisk2_codes,
    aqc.all_qrisk2_displays
FROM {{ ref('int_ltc_lcs_cf_cvd_65_base_population') }} bp
LEFT JOIN all_qrisk2_codes aqc USING (person_id) 