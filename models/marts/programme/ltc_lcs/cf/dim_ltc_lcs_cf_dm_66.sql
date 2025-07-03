{{ config(
    materialized='table',
    post_hook=[
        "COMMENT ON TABLE {{ this }} IS 'Mart: LTC LCS Case Finding DM_66 - Identifies patients with elevated HbA1c in pre-diabetic range with recent monitoring requiring continued surveillance.

Business Purpose:
• Support systematic case finding for patients with pre-diabetic HbA1c levels requiring ongoing monitoring
• Enable continued diabetes surveillance in patients with recent elevated glucose control
• Provide clinical decision support for diabetes prevention in patients with active monitoring
• Support quality improvement initiatives for diabetes prevention through enhanced surveillance

Data Granularity:
• One row per person with latest HbA1c between 42-46 mmol/mol assessed within 12 months
• Includes patients with pre-diabetic glucose control undergoing active monitoring
• Limited to patients with recent assessment showing elevated glucose levels

Key Features:
• Pre-diabetic HbA1c range identification (42-46 mmol/mol) for active diabetes prevention
• Recent monitoring confirmation (within 12 months) for current clinical status
• Evidence-based case finding supporting diabetes prevention in actively monitored populations
• Integration with diabetes prevention pathways for sustained intervention and monitoring'"
    ]
) }}

-- Intermediate model for LTC LCS CF DM_66 case finding
-- Patients who meet ALL of the following criteria:
-- 1. Latest HbA1c reading between 42 and 46 mmol/mol (inclusive)
-- 2. HbA1c reading must be within the last 12 months

WITH base_population AS (
    -- Get base population aged 17+ (already excludes LTC registers and NHS health checks)
    SELECT DISTINCT
        person_id,
        age
    FROM {{ ref('int_ltc_lcs_cf_base_population') }}
    WHERE age >= 17
),

hba1c_readings AS (
    -- Get all HbA1c readings within last 12 months
    SELECT
        person_id,
        clinical_effective_date,
        result_value,
        mapped_concept_code,
        mapped_concept_display
    FROM {{ ref('int_ltc_lcs_dm_observations') }}
    WHERE
        cluster_id = 'HBA1C_LEVEL'
        AND result_value > 0
        AND clinical_effective_date >= DATEADD(YEAR, -1, CURRENT_DATE())
),

latest_hba1c AS (
    -- Get the most recent HbA1c reading for each person
    SELECT
        person_id,
        clinical_effective_date AS latest_hba1c_date,
        result_value AS latest_hba1c_value,
        ARRAY_AGG(DISTINCT mapped_concept_code) WITHIN GROUP (
            ORDER BY mapped_concept_code
        ) AS all_hba1c_codes,
        ARRAY_AGG(DISTINCT mapped_concept_display) WITHIN GROUP (
            ORDER BY mapped_concept_display
        ) AS all_hba1c_displays
    FROM hba1c_readings
    GROUP BY person_id, clinical_effective_date, result_value
    QUALIFY
        ROW_NUMBER()
            OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC)
        = 1
)

-- Final selection with HbA1c range assessment
SELECT
    bp.person_id,
    bp.age,
    hba1c.latest_hba1c_date,
    hba1c.latest_hba1c_value,
    hba1c.all_hba1c_codes,
    hba1c.all_hba1c_displays,
    COALESCE(
        hba1c.latest_hba1c_value >= 42 AND hba1c.latest_hba1c_value <= 46,
        FALSE
    ) AS has_elevated_hba1c
FROM base_population AS bp
LEFT JOIN latest_hba1c AS hba1c ON bp.person_id = hba1c.person_id
WHERE
    hba1c.latest_hba1c_value >= 42
    AND hba1c.latest_hba1c_value <= 46
