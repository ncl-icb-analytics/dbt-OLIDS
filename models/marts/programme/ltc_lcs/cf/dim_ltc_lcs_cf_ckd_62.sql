/*
LTC LCS Case Finding: CKD_62 - Consecutive High UACR Readings

Purpose:
- Identifies patients with two consecutive UACR readings above 4 (case finding for undiagnosed CKD)

Business Logic:
1. Base Population:
   - Patients aged 17+ from base population (excludes those on CKD and Diabetes registers)

2. UACR Criteria:
   - Must have at least 2 UACR readings with values > 0
   - Takes max value per day to handle multiple readings
   - Filters out adjacent day duplicates (same result on consecutive days)
   - Both most recent and previous reading must be > 4
   - Uses 'UINE_ACR' cluster ID

3. Output:
   - Only includes patients who meet all criteria (both readings > 4)
   - Provides latest and previous UACR values and dates
   - Collects all UACR concept codes and displays for traceability

Implementation Notes:
- Materialized as ephemeral to avoid cluttering the database
- Uses LAG functions to identify consecutive readings
- Implements adjacent day filtering logic

Dependencies:
- int_ltc_lcs_cf_base_population: For base population (age >= 17)
- int_ltc_lcs_ckd_observations: For UACR readings

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

uacr_with_adjacent_check AS (
    -- Check for same results on adjacent days
    SELECT
        *,
        CASE
            WHEN
                dateadd(DAY, 1, clinical_effective_date)
                = lag(clinical_effective_date)
                    OVER (
                        PARTITION BY person_id
                        ORDER BY clinical_effective_date DESC
                    )
                AND result_value
                = lag(result_value)
                    OVER (
                        PARTITION BY person_id
                        ORDER BY clinical_effective_date DESC
                    )
                THEN 'EXCLUDE'
            ELSE 'INCLUDE'
        END AS adjacent_day_check
    FROM uacr_readings
),

uacr_filtered AS (
    -- Remove adjacent day duplicates
    SELECT *
    FROM uacr_with_adjacent_check
    WHERE adjacent_day_check = 'INCLUDE'
),

uacr_ranked AS (
    -- Rank UACR readings by date for each person
    SELECT
        *,
        row_number()
            OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC)
            AS reading_rank
    FROM uacr_filtered
),

uacr_counts AS (
    -- Count readings per person to ensure at least 2
    SELECT
        person_id,
        count(*) AS reading_count
    FROM uacr_filtered
    GROUP BY person_id
    HAVING count(*) > 1
),

uacr_with_lags AS (
    -- Get the two most recent readings with their lags
    SELECT
        ur.person_id,
        ur.clinical_effective_date AS latest_uacr_date,
        ur.result_value AS latest_uacr_value,
        lag(ur.clinical_effective_date)
            OVER (
                PARTITION BY ur.person_id
                ORDER BY ur.clinical_effective_date DESC
            )
            AS previous_uacr_date,
        lag(ur.result_value)
            OVER (
                PARTITION BY ur.person_id
                ORDER BY ur.clinical_effective_date DESC
            )
            AS previous_uacr_value
    FROM uacr_ranked AS ur
    INNER JOIN uacr_counts ON ur.person_id = uacr_counts.person_id
    WHERE ur.reading_rank <= 2
    QUALIFY ur.reading_rank = 1
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
    ceg.previous_uacr_date,
    ceg.latest_uacr_value,
    ceg.previous_uacr_value,
    codes.all_uacr_codes,
    codes.all_uacr_displays,
    coalesce(
        ceg.latest_uacr_value > 4 AND ceg.previous_uacr_value > 4,
        FALSE
    ) AS has_elevated_uacr,
    -- Meets criteria flag for mart model
    coalesce(
        ceg.latest_uacr_value > 4 AND ceg.previous_uacr_value > 4,
        FALSE
    ) AS meets_criteria
FROM base_population AS bp
LEFT JOIN uacr_with_lags AS ceg ON bp.person_id = ceg.person_id
LEFT JOIN uacr_codes AS codes ON bp.person_id = codes.person_id
WHERE
    ceg.latest_uacr_value > 4
    AND ceg.previous_uacr_value > 4
