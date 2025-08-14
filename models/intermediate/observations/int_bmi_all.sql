{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All BMI measurements including both recorded BMI values and calculated BMI from HEIGHT/WEIGHT.
Includes ALL persons (active, inactive, deceased) with basic validation (5-400 range).
Calculates fresh BMI when height/weight measurements are on different dates than recorded BMI.
*/

WITH recorded_bmi AS (
    -- Recorded BMI observations from BMIVAL_COD cluster
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        CAST(obs.result_value AS NUMBER(10,2)) AS bmi_value,
        obs.result_unit_display,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        obs.result_value,
        'recorded' AS bmi_source

    FROM ({{ get_observations("'BMIVAL_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
      AND obs.result_value IS NOT NULL
),

height_measurements AS (
    -- Get height measurements in cm
    SELECT
        obs.person_id,
        obs.clinical_effective_date,
        obs.observation_id,
        CAST(obs.result_value AS NUMBER(15,2)) AS height_cm,
        obs.result_unit_display AS height_unit
    FROM ({{ get_observations("'HEIGHT'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
      AND obs.result_value IS NOT NULL
      AND CAST(obs.result_value AS NUMBER(15,2)) BETWEEN 50 AND 250  -- Valid height range in cm
),

weight_measurements AS (
    -- Get weight measurements in kg
    SELECT
        obs.person_id,
        obs.clinical_effective_date,
        obs.observation_id,
        CAST(obs.result_value AS NUMBER(15,2)) AS weight_kg,
        obs.result_unit_display AS weight_unit
    FROM ({{ get_observations("'WEIGHT'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
      AND obs.result_value IS NOT NULL
      AND CAST(obs.result_value AS NUMBER(15,2)) BETWEEN 10 AND 500  -- Valid weight range in kg
),

latest_height_per_person AS (
    -- Get the most recent height for each person
    SELECT
        person_id,
        height_cm,
        clinical_effective_date AS height_date,
        observation_id AS height_obs_id
    FROM height_measurements
    QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC) = 1
),

height_weight_pairs AS (
    -- Pair each weight with the person's most recent height
    SELECT
        w.person_id,
        w.clinical_effective_date,
        h.height_cm,
        w.weight_kg,
        -- Calculate BMI: weight (kg) / (height (cm) / 100)²
        CASE 
            WHEN h.height_cm > 0 THEN ROUND(w.weight_kg / ((h.height_cm / 100.0) * (h.height_cm / 100.0)), 2)
            ELSE NULL
        END AS calculated_bmi,
        h.height_obs_id,
        w.observation_id AS weight_obs_id,
        h.height_date
    FROM weight_measurements w
    INNER JOIN latest_height_per_person h
        ON w.person_id = h.person_id
),

calculated_bmi AS (
    -- Create calculated BMI records from height/weight pairs
    SELECT
        'CALC_' || CAST(hw.height_obs_id AS VARCHAR) || '_' || CAST(hw.weight_obs_id AS VARCHAR) AS observation_id,
        hw.person_id,
        hw.clinical_effective_date,
        hw.calculated_bmi AS bmi_value,
        'kg/m²' AS result_unit_display,
        'CALCULATED_BMI' AS concept_code,
        'Calculated BMI from Height/Weight' AS concept_display,
        'CALCULATED' AS source_cluster_id,
        CAST(hw.calculated_bmi AS VARCHAR(20)) AS result_value,
        'calculated' AS bmi_source
    FROM height_weight_pairs hw
    WHERE hw.calculated_bmi IS NOT NULL
      AND hw.calculated_bmi BETWEEN 5 AND 400  -- Valid BMI range
),

all_bmi AS (
    -- Combine recorded and calculated BMI
    SELECT * FROM recorded_bmi
    UNION ALL
    SELECT * FROM calculated_bmi
)

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    bmi_value,
    result_unit_display,
    concept_code,
    concept_display,
    source_cluster_id,
    result_value,
    bmi_source,

    -- Data quality validation
    CASE
        WHEN bmi_value BETWEEN 5 AND 400 THEN TRUE
        ELSE FALSE
    END AS is_valid_bmi,

    -- Clinical categorisation (only for valid BMI)
    CASE
        WHEN bmi_value NOT BETWEEN 5 AND 400 THEN 'Invalid'
        WHEN bmi_value < 18.5 THEN 'Underweight'
        WHEN bmi_value < 25 THEN 'Normal'
        WHEN bmi_value < 30 THEN 'Overweight'
        WHEN bmi_value < 35 THEN 'Obese Class I'
        WHEN bmi_value < 40 THEN 'Obese Class II'
        ELSE 'Obese Class III'
    END AS bmi_category,

    -- BMI risk sort key (higher number = higher risk)
    CASE
        WHEN bmi_value NOT BETWEEN 5 AND 400 THEN 0  -- Invalid
        WHEN bmi_value < 18.5 THEN 2  -- Underweight - Health risk
        WHEN bmi_value < 25 THEN 1  -- Normal - Baseline/lowest risk
        WHEN bmi_value < 30 THEN 3  -- Overweight - Moderate risk
        WHEN bmi_value < 35 THEN 4  -- Obese Class I - High risk
        WHEN bmi_value < 40 THEN 5  -- Obese Class II - Higher risk
        ELSE 6  -- Obese Class III - Highest risk
    END AS bmi_risk_sort_key

FROM all_bmi

-- Sort for consistent output
ORDER BY person_id, clinical_effective_date DESC
