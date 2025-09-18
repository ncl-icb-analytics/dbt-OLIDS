/*
LTC LCS Case Finding: CKD_64 - Specific Conditions Requiring eGFR Monitoring

Purpose:
- Identifies patients with specific conditions (AKI, BPH/Gout, Lithium, Microhaematuria)
  who have not had eGFR in last 12 months

Business Logic:
1. Base Population:
   - Patients aged 17+ from base population (excludes those on CKD and Diabetes registers)

2. Condition Criteria (must have at least one):
   - AKI in last 3 years ('CKD_AKI')
   - BPH or Gout ('CKD_BPH_GOUT')
   - Lithium/Sulfasalazine/Tacrolimus medications in last 6 months
   - Valid microhaematuria (complex logic with UACR and urine tests)

3. eGFR Exclusion:
   - Must NOT have had eGFR test in last 12 months

4. Microhaematuria Validation:
   - Has microhaematuria ('Hematuria')
   - AND either: no negative urine test after haematuria OR has UACR > 30 after haematuria

Implementation Notes:
- Materialized as ephemeral to avoid cluttering the database
- Complex microhaematuria validation logic from legacy
- Mirrors legacy logic exactly

Dependencies:
- int_ltc_lcs_cf_base_population: For base population (age >= 17)
- int_ltc_lcs_ckd_observations: For clinical events and lab results

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

clinical_events AS (
    -- Get all relevant clinical events in one go
    SELECT
        person_id,
        clinical_effective_date,
        cluster_id,
        cast(result_value AS number) AS result_value,
        mapped_concept_code AS concept_code,
        mapped_concept_display AS concept_display,
        -- Flag each type of event
        coalesce(
            cluster_id = 'CKD_ACUTE_KIDNEY_INJURY'
            AND clinical_effective_date >= dateadd(MONTH, -36, current_date()),
            FALSE
        ) AS is_aki,
        coalesce(cluster_id = 'CKD_BPH_GOUT', FALSE) AS is_bph_gout,
        coalesce(cluster_id IN (
            'LITHIUM_MEDICATIONS',
            'SULFASALAZINE_MEDICATIONS',
            'TACROLIMUS_MEDICATIONS'
        )
        AND clinical_effective_date >= dateadd(MONTH, -6, current_date()),
        FALSE) AS is_lithium,
        coalesce(cluster_id = 'HAEMATURIA', FALSE) AS is_microhaematuria,
        coalesce(
            cluster_id = 'UACR_TESTING' AND result_value > 30,
            FALSE
        ) AS is_uacr_high,
        coalesce(cluster_id IN (
            'URINE_BLOOD_NEGATIVE', 'PROTEINURIA_FINDINGS'
        ),
        FALSE) AS is_urine_test
    FROM {{ ref('int_ltc_lcs_ckd_observations') }}
    WHERE cluster_id IN (
        'CKD_AKI',
        'CKD_BPH_GOUT',
        'LITHIUM_MEDICATIONS',
        'SULFASALAZINE_MEDICATIONS',
        'TACROLIMUS_MEDICATIONS',
        'Hematuria',
        'UACR_TESTING',
        'URINE_BLOOD_NEGATIVE',
        'PROTEINURIA_FINDINGS'
    )
),

condition_summary AS (
    -- Summarise conditions per person
    SELECT
        person_id,
        -- AKI
        max(CASE WHEN is_aki THEN clinical_effective_date END)
            AS latest_aki_date,
        boolor_agg(is_aki) AS has_acute_kidney_injury,
        -- BPH/Gout
        max(CASE WHEN is_bph_gout THEN clinical_effective_date END)
            AS latest_bph_gout_date,
        boolor_agg(is_bph_gout) AS has_bph_gout,
        -- Lithium
        max(CASE WHEN is_lithium THEN clinical_effective_date END)
            AS latest_lithium_date,
        boolor_agg(is_lithium) AS has_lithium_medication,
        -- Microhaematuria
        max(CASE WHEN is_microhaematuria THEN clinical_effective_date END)
            AS latest_microhaematuria_date,
        boolor_agg(is_microhaematuria) AS has_microhaematuria,
        -- UACR
        max(CASE WHEN is_uacr_high THEN clinical_effective_date END)
            AS latest_uacr_date,
        max(CASE WHEN is_uacr_high THEN result_value END) AS latest_uacr_value,
        -- Urine tests
        max(CASE WHEN is_urine_test THEN clinical_effective_date END)
            AS latest_urine_test_date,
        -- Codes and displays
        array_agg(DISTINCT concept_code) WITHIN GROUP (ORDER BY concept_code)
            AS all_condition_codes,
        array_agg(DISTINCT concept_display) WITHIN GROUP (
            ORDER BY concept_display
        ) AS all_condition_displays
    FROM clinical_events
    GROUP BY person_id
),

egfr_in_last_year AS (
    -- Get patients with eGFR in last 12 months to exclude
    SELECT DISTINCT person_id
    FROM {{ ref('int_ltc_lcs_ckd_observations') }}
    WHERE
        cluster_id IN ('EGFR_COD_LCS', 'EGFR_COD')
        AND result_value IS NOT NULL
        AND cast(result_value AS number) > 0
        AND clinical_effective_date >= dateadd(MONTH, -12, current_date())
),

microhaematuria_with_conditions AS (
    -- Get patients with microhaematuria and specific conditions
    SELECT
        cs.*,
        coalesce(
            cs.latest_urine_test_date IS NULL
            OR cs.latest_microhaematuria_date > cs.latest_urine_test_date
            OR (
                cs.latest_uacr_date IS NOT NULL
                AND cs.latest_uacr_date >= cs.latest_microhaematuria_date
            ), FALSE
        ) AS has_valid_microhaematuria
    FROM condition_summary AS cs
)

-- Final selection
SELECT
    bp.person_id,
    bp.age,
    mh.latest_aki_date,
    mh.latest_bph_gout_date,
    mh.latest_lithium_date,
    mh.latest_microhaematuria_date,
    mh.latest_uacr_date,
    mh.latest_uacr_value,
    mh.all_condition_codes,
    mh.all_condition_displays,
    coalesce(mh.has_acute_kidney_injury, FALSE) AS has_acute_kidney_injury,
    coalesce(mh.has_bph_gout, FALSE) AS has_bph_gout,
    coalesce(mh.has_lithium_medication, FALSE) AS has_lithium_medication,
    coalesce(mh.has_valid_microhaematuria, FALSE) AS has_microhaematuria,
    -- Meets criteria flag for mart model
    coalesce((
        coalesce(mh.has_acute_kidney_injury, FALSE)
        OR coalesce(mh.has_bph_gout, FALSE)
        OR coalesce(mh.has_lithium_medication, FALSE)
        OR coalesce(mh.has_valid_microhaematuria, FALSE)
    ), FALSE) AS meets_criteria
FROM base_population AS bp
LEFT JOIN microhaematuria_with_conditions AS mh ON bp.person_id = mh.person_id
WHERE NOT EXISTS (
    SELECT 1 FROM egfr_in_last_year AS egfr
    WHERE egfr.person_id = bp.person_id
)
AND (
    coalesce(mh.has_acute_kidney_injury, FALSE)
    OR coalesce(mh.has_bph_gout, FALSE)
    OR coalesce(mh.has_lithium_medication, FALSE)
    OR coalesce(mh.has_valid_microhaematuria, FALSE)
)
