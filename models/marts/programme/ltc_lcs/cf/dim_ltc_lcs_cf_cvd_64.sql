{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} SET COMMENT = 'CVD_64 case finding: Patients with specific cardiovascular medication patterns'"
) }}

-- CVD_64 case finding: High-dose statin case finding
-- Identifies patients from the general CVD base population who need high-dose statins

WITH high_dose_statin_medications AS (
    -- Get all high-dose statin medications for patients in base
    SELECT
        med.person_id,
        med.order_date AS clinical_effective_date,
        med.mapped_concept_code,
        med.mapped_concept_display,
        ROW_NUMBER()
            OVER (PARTITION BY med.person_id ORDER BY med.order_date DESC)
            AS medication_rank
    FROM {{ ref('int_ltc_lcs_cvd_medications') }} AS med
    INNER JOIN {{ ref('int_ltc_lcs_cf_cvd_base_population') }} AS base ON med.person_id = base.person_id
    WHERE med.cluster_id = 'STATIN_CVD_64_MEDICATIONS'
),

latest_high_dose_statins AS (
    -- Get latest high-dose statin for each person
    SELECT
        person_id,
        clinical_effective_date AS latest_statin_date,
        mapped_concept_code AS latest_statin_code,
        mapped_concept_display AS latest_statin_display
    FROM high_dose_statin_medications
    WHERE medication_rank = 1
),

all_high_dose_statin_codes AS (
    -- Aggregate all high-dose statin codes and displays for each person
    SELECT
        person_id,
        ARRAY_AGG(DISTINCT mapped_concept_code) WITHIN GROUP (
            ORDER BY mapped_concept_code
        ) AS all_statin_codes,
        ARRAY_AGG(DISTINCT mapped_concept_display) WITHIN GROUP (
            ORDER BY mapped_concept_display
        ) AS all_statin_displays
    FROM high_dose_statin_medications
    GROUP BY person_id
)

-- Final selection: patients from CVD base who need high-dose statins
SELECT
    bp.person_id,
    bp.age,
    TRUE AS needs_high_dose_statin,  -- All patients in base need high-dose statins
    lhs.latest_statin_date,
    lhs.latest_statin_code,
    lhs.latest_statin_display,
    ahsc.all_statin_codes,
    ahsc.all_statin_displays
FROM {{ ref('int_ltc_lcs_cf_cvd_base_population') }} AS bp
LEFT JOIN latest_high_dose_statins AS lhs ON bp.person_id = lhs.person_id
LEFT JOIN all_high_dose_statin_codes AS ahsc ON bp.person_id = ahsc.person_id
