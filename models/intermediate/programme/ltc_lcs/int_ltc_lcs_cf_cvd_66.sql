{{ config(materialized='ephemeral') }}

-- CVD_66 case finding: Statin review case finding
-- Identifies patients aged 75-83 who need statin review (no recent QRISK2 assessment)

WITH qrisk2_readings AS (
    -- Get all QRISK2 readings > 0 for patients in base
    SELECT
        obs.person_id,
        obs.clinical_effective_date,
        obs.result_value,
        obs.mapped_concept_code,
        obs.mapped_concept_display,
        ROW_NUMBER() OVER (PARTITION BY obs.person_id ORDER BY obs.clinical_effective_date DESC) AS reading_rank
    FROM {{ ref('int_ltc_lcs_cvd_observations') }} obs
    JOIN {{ ref('int_ltc_lcs_cf_cvd_66_base_population') }} bp USING (person_id)
    WHERE obs.cluster_id = 'QRISK2_10YEAR'
        AND obs.result_value > 0
),

latest_qrisk2_readings AS (
    -- Get latest QRISK2 reading for each person
    SELECT
        person_id,
        clinical_effective_date AS latest_qrisk2_date,
        result_value AS latest_qrisk2_value,
        mapped_concept_code AS latest_qrisk2_code,
        mapped_concept_display AS latest_qrisk2_display
    FROM qrisk2_readings
    WHERE reading_rank = 1
),

all_qrisk2_codes AS (
    -- Aggregate all QRISK2 codes and displays for each person
    SELECT
        person_id,
        ARRAY_AGG(DISTINCT mapped_concept_code) WITHIN GROUP (ORDER BY mapped_concept_code) AS all_qrisk2_codes,
        ARRAY_AGG(DISTINCT mapped_concept_display) WITHIN GROUP (ORDER BY mapped_concept_display) AS all_qrisk2_displays
    FROM qrisk2_readings
    GROUP BY person_id
)

-- Final selection: patients from CVD_66 base who need statin review (no QRISK2 assessment)
SELECT
    bp.person_id,
    bp.age,
    CASE 
        WHEN lqr.person_id IS NULL THEN TRUE  -- No QRISK2 assessment
        ELSE FALSE
    END AS needs_statin_review,
    lqr.latest_qrisk2_date,
    lqr.latest_qrisk2_value,
    lqr.latest_qrisk2_code,
    lqr.latest_qrisk2_display,
    aqc.all_qrisk2_codes,
    aqc.all_qrisk2_displays
FROM {{ ref('int_ltc_lcs_cf_cvd_66_base_population') }} bp
LEFT JOIN latest_qrisk2_readings lqr USING (person_id)
LEFT JOIN all_qrisk2_codes aqc USING (person_id)
WHERE lqr.person_id IS NULL  -- Only include patients with no QRISK2 assessment 