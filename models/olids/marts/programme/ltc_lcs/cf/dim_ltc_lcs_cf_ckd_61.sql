/*
LTC LCS Case Finding: CKD_61 - Consecutive Low eGFR Readings

Purpose:
- Identifies patients with two consecutive eGFR readings below 60 (case finding for undiagnosed CKD)

Business Logic:
1. Base Population:
   - Patients aged 17+ from base population (excludes those on CKD and Diabetes registers)

2. eGFR Criteria:
   - Must have at least 2 eGFR readings with values > 0
   - Both most recent and previous reading must be < 60
   - Uses 'EGFR_COD_LCS' and 'EGFR_COD' cluster IDs for eGFR testing

3. Output:
   - Only includes patients who meet all criteria (both readings < 60)
   - Provides latest and previous eGFR values and dates
   - Collects all eGFR concept codes and displays for traceability

Implementation Notes:
- Materialized as table for consumption by downstream models
- Uses LAG functions to identify consecutive readings

Dependencies:
- int_ltc_lcs_cf_base_population: For base population (age >= 17)
- int_ltc_lcs_ckd_observations: For eGFR readings

*/

{{ config(
    materialized='table') }}

WITH base_population AS (
    -- Get base population of patients over 17
    -- Base population already excludes those on CKD and Diabetes registers
    SELECT DISTINCT
        person_id,
        age
    FROM {{ ref('int_ltc_lcs_cf_base_population') }}
    WHERE age >= 17
),

egfr_readings AS (
    -- Get all eGFR readings with values > 0
    SELECT
        person_id,
        clinical_effective_date,
        cast(result_value AS number) AS result_value,
        mapped_concept_code AS concept_code,
        mapped_concept_display AS concept_display
    FROM {{ ref('int_ltc_lcs_ckd_observations') }}
    WHERE
        cluster_id IN ('EGFR_COD_LCS', 'EGFR_COD')
        AND result_value IS NOT NULL
        AND cast(result_value AS number) > 0
),

egfr_ranked AS (
    -- Rank eGFR readings by date for each person
    SELECT
        *,
        row_number()
            OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC)
            AS reading_rank
    FROM egfr_readings
),

egfr_counts AS (
    -- Count readings per person to ensure at least 2
    SELECT
        person_id,
        count(*) AS reading_count
    FROM egfr_readings
    GROUP BY person_id
    HAVING count(*) > 1
),

egfr_with_lags AS (
    -- Get the two most recent readings with their lags
    SELECT
        er.person_id,
        er.clinical_effective_date AS latest_egfr_date,
        er.result_value AS latest_egfr_value,
        lag(er.clinical_effective_date)
            OVER (
                PARTITION BY er.person_id
                ORDER BY er.clinical_effective_date DESC
            )
            AS previous_egfr_date,
        lag(er.result_value)
            OVER (
                PARTITION BY er.person_id
                ORDER BY er.clinical_effective_date DESC
            )
            AS previous_egfr_value
    FROM egfr_ranked AS er
    INNER JOIN egfr_counts ON er.person_id = egfr_counts.person_id
    WHERE er.reading_rank <= 2
    QUALIFY er.reading_rank = 1
),

egfr_codes AS (
-- Get all codes and displays for each person
    SELECT
        person_id,
        array_agg(DISTINCT concept_code) WITHIN GROUP (ORDER BY concept_code)
            AS all_egfr_codes,
        array_agg(DISTINCT concept_display) WITHIN GROUP (
            ORDER BY concept_display
        ) AS all_egfr_displays
    FROM egfr_readings
    GROUP BY person_id
)

-- Final selection
SELECT
    bp.person_id,
    bp.age,
    ceg.latest_egfr_date,
    ceg.previous_egfr_date,
    ceg.latest_egfr_value,
    ceg.previous_egfr_value,
    codes.all_egfr_codes,
    codes.all_egfr_displays,
    coalesce(
        ceg.latest_egfr_value < 60 AND ceg.previous_egfr_value < 60,
        FALSE
    ) AS has_ckd,
    -- Meets criteria flag for mart model
    coalesce(
        ceg.latest_egfr_value < 60 AND ceg.previous_egfr_value < 60,
        FALSE
    ) AS meets_criteria
FROM base_population AS bp
LEFT JOIN egfr_with_lags AS ceg ON bp.person_id = ceg.person_id
LEFT JOIN egfr_codes AS codes ON bp.person_id = codes.person_id
WHERE
    ceg.latest_egfr_value < 60
    AND ceg.previous_egfr_value < 60
