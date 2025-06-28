{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

/*
BMI measurements with QOF-specific rules for obesity register eligibility.
Includes both numeric BMI values (BMIVAL_COD) and BMI30+ codes (BMI30_COD).
Implements specific obesity register logic and ethnicity-adjusted thresholds.
*/

WITH base_observations AS (

    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        obs.result_value AS original_result_value,

        -- Extract BMI value from result_value, handling both numeric and coded values
        CASE
            WHEN obs.cluster_id = 'BMIVAL_COD' THEN CAST(obs.result_value AS NUMBER(10,2))
            WHEN obs.cluster_id = 'BMI30_COD' THEN 30 -- BMI30_COD implies BMI >= 30
            ELSE NULL
        END AS bmi_value

    FROM ({{ get_observations("'BMI30_COD', 'BMIVAL_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

validated_observations AS (

    SELECT
        *,

        -- Data quality flags
        CASE
            WHEN bmi_value BETWEEN 5 AND 400 THEN TRUE
            ELSE FALSE
        END AS is_valid_bmi,

        -- QOF obesity register flags
        CASE
            WHEN source_cluster_id = 'BMI30_COD' OR bmi_value >= 30 THEN TRUE
            ELSE FALSE
        END AS is_bmi_30_plus,

        CASE
            WHEN bmi_value >= 27.5 THEN TRUE
            ELSE FALSE
        END AS is_bmi_27_5_plus,

        CASE
            WHEN bmi_value >= 25 THEN TRUE
            ELSE FALSE
        END AS is_bmi_25_plus

    FROM base_observations
    WHERE bmi_value IS NOT NULL
),

person_level_aggregation AS (

    SELECT
        person_id,

        -- Latest BMI information
        MAX(clinical_effective_date) AS latest_bmi_date,
        MAX(CASE WHEN is_valid_bmi THEN clinical_effective_date END) AS latest_valid_bmi_date,

        -- QOF flags based on any qualifying observation
        MAX(is_bmi_30_plus::int)::boolean AS has_bmi_30_plus_ever,
        MAX(is_bmi_27_5_plus::int)::boolean AS has_bmi_27_5_plus_ever,

        -- Aggregate concept information
        ARRAY_AGG(DISTINCT concept_code) AS all_bmi_concept_codes,
        ARRAY_AGG(DISTINCT concept_display) AS all_bmi_concept_displays

    FROM validated_observations
    GROUP BY person_id
),

latest_valid_bmi AS (

    SELECT
        person_id,
        bmi_value AS latest_valid_bmi_value,
        ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC) AS rn
    FROM validated_observations
    WHERE is_valid_bmi = TRUE
),

latest_observations AS (

    SELECT
        vo.*,
        ROW_NUMBER() OVER (PARTITION BY vo.person_id ORDER BY vo.clinical_effective_date DESC) AS rn
    FROM validated_observations vo
)

-- Final selection with person-level QOF flags
SELECT
    lo.person_id,
    lo.observation_id,
    lo.clinical_effective_date,
    lo.concept_code,
    lo.concept_display,
    lo.source_cluster_id,
    lo.bmi_value,
    lo.is_bmi_30_plus,
    lo.is_bmi_27_5_plus,
    lo.is_bmi_25_plus,
    lo.is_valid_bmi,
    lo.original_result_value,

    -- Person-level aggregated data
    pla.latest_bmi_date,
    pla.latest_valid_bmi_date,
    lvb.latest_valid_bmi_value,
    pla.has_bmi_30_plus_ever,
    pla.has_bmi_27_5_plus_ever,
    pla.all_bmi_concept_codes,
    pla.all_bmi_concept_displays

FROM latest_observations lo
LEFT JOIN person_level_aggregation pla ON lo.person_id = pla.person_id
LEFT JOIN latest_valid_bmi lvb ON lo.person_id = lvb.person_id AND lvb.rn = 1
WHERE lo.rn = 1
