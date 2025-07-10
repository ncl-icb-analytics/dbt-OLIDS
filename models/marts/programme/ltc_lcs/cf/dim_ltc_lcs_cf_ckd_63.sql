/*
LTC LCS Case Finding: CKD_63 - Elevated UACR (Single Reading)

Purpose:
- Identifies patients with latest UACR reading above 70 (case finding for undiagnosed CKD)
- Excludes patients already captured in CKD_62 (consecutive high readings)

Business Logic:
1. Base Population:
   - Patients aged 17+ from base population (excludes those on CKD and Diabetes registers)
   - Excludes patients already in CKD_62

2. UACR Criteria:
   - Latest UACR reading must be > 70
   - Takes max value per day to handle multiple readings
   - Uses 'UACR_TESTING' cluster ID

3. Output:
   - Only includes patients who meet all criteria (latest reading > 70)
   - Provides latest UACR value and date
   - Collects all UACR concept codes and displays for traceability

Implementation Notes:
- Materialized as ephemeral to avoid cluttering the database
- Excludes patients from CKD_62 to avoid double counting

Dependencies:
- int_ltc_lcs_cf_base_population: For base population (age >= 17)
- int_ltc_lcs_ckd_observations: For UACR readings
- dim_prog_ltc_lcs_cf_ckd_62: To exclude patients with consecutive high readings

*/

{{ config(
    materialized='table') }}

WITH base_population AS (
    -- Get base population of patients over 17
    -- Excludes those on CKD and Diabetes registers, and those in CKD_62
    SELECT DISTINCT
        bp.person_id,
        bp.age
    FROM {{ ref('int_ltc_lcs_cf_base_population') }} AS bp
    LEFT JOIN
        {{ ref('dim_ltc_lcs_cf_ckd_62') }} AS ckd62
        ON bp.person_id = ckd62.person_id
    WHERE
        bp.age >= 17
        AND ckd62.person_id IS NULL -- Exclude patients in CKD_62
),

uacr_readings AS (
-- Get all UACR readings with values > 0
-- Take max value per day to handle multiple readings
    SELECT
        person_id,
        clinical_effective_date,
        max(cast(result_value AS number)) AS result_value,
        any_value(mapped_concept_code) AS concept_code,
        any_value(mapped_concept_display) AS concept_display
    FROM {{ ref('int_ltc_lcs_ckd_observations') }}
    WHERE
        cluster_id = 'UACR_TESTING'
        AND result_value IS NOT NULL
        AND cast(result_value AS number) > 0
    GROUP BY
        person_id,
        clinical_effective_date
),

latest_uacr AS (
-- Get the most recent UACR reading for each person
    SELECT
        ur.person_id,
        ur.clinical_effective_date AS latest_uacr_date,
        ur.result_value AS latest_uacr_value
    FROM uacr_readings AS ur
    QUALIFY
        row_number()
            OVER (
                PARTITION BY ur.person_id
                ORDER BY ur.clinical_effective_date DESC
            )
        = 1
),

uacr_codes AS (
-- Get all codes and displays for each person
    SELECT
        person_id,
        array_agg(DISTINCT concept_code) WITHIN GROUP (ORDER BY concept_code)
            AS all_uacr_codes,
        array_agg(DISTINCT concept_display) WITHIN GROUP (
            ORDER BY concept_display
        ) AS all_uacr_displays
    FROM uacr_readings
    GROUP BY person_id
)

-- Final selection
SELECT
    bp.person_id,
    bp.age,
    ceg.latest_uacr_date,
    ceg.latest_uacr_value,
    codes.all_uacr_codes,
    codes.all_uacr_displays,
    coalesce(ceg.latest_uacr_value > 70, FALSE) AS has_elevated_uacr,
    -- Meets criteria flag for mart model
    coalesce(ceg.latest_uacr_value > 70, FALSE) AS meets_criteria
FROM base_population AS bp
LEFT JOIN latest_uacr AS ceg ON bp.person_id = ceg.person_id
LEFT JOIN uacr_codes AS codes ON bp.person_id = codes.person_id
WHERE ceg.latest_uacr_value > 70
