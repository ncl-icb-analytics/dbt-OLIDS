{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All BMI measurements for adults aged 18+ including both recorded BMI values and calculated BMI from HEIGHT/WEIGHT.
Includes ALL persons (active, inactive, deceased) aged 18+ with basic validation (10-150 range).
Uses ethnicity-adjusted BMI categories per NICE NG246 for cardiometabolic risk populations.
Avoids calculating BMI on dates where recorded BMI already exists for the same person.
Age restriction: Adult BMI categories are only clinically appropriate for ages 18+.
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

weight_with_prior_height AS (
    -- For each weight, find the most recent height at or before that weight's date
    SELECT
        w.person_id,
        w.clinical_effective_date,
        w.weight_kg,
        w.observation_id AS weight_obs_id,
        h.height_cm,
        h.observation_id AS height_obs_id,
        h.clinical_effective_date AS height_date,
        -- Calculate BMI: weight (kg) / (height (cm) / 100)²
        CASE 
            WHEN h.height_cm > 0 THEN ROUND(w.weight_kg / ((h.height_cm / 100.0) * (h.height_cm / 100.0)), 2)
            ELSE NULL
        END AS calculated_bmi,
        'weight_primary' AS calculation_source
    FROM weight_measurements w
    INNER JOIN height_measurements h
        ON w.person_id = h.person_id
        AND h.clinical_effective_date <= w.clinical_effective_date
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY w.person_id, w.clinical_effective_date, w.observation_id 
        ORDER BY h.clinical_effective_date DESC
    ) = 1
),

height_with_prior_weight AS (
    -- For each height, find the most recent weight at or before that height's date
    -- Exclude combinations already captured in weight_with_prior_height
    SELECT
        h.person_id,
        h.clinical_effective_date,
        w.weight_kg,
        w.observation_id AS weight_obs_id,
        h.height_cm,
        h.observation_id AS height_obs_id,
        w.clinical_effective_date AS weight_date,
        -- Calculate BMI: weight (kg) / (height (cm) / 100)²
        CASE 
            WHEN h.height_cm > 0 THEN ROUND(w.weight_kg / ((h.height_cm / 100.0) * (h.height_cm / 100.0)), 2)
            ELSE NULL
        END AS calculated_bmi,
        'height_primary' AS calculation_source
    FROM height_measurements h
    INNER JOIN weight_measurements w
        ON h.person_id = w.person_id
        AND w.clinical_effective_date <= h.clinical_effective_date
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY h.person_id, h.clinical_effective_date, h.observation_id 
        ORDER BY w.clinical_effective_date DESC
    ) = 1
),

all_calculated_pairs AS (
    SELECT * FROM weight_with_prior_height
    UNION ALL
    SELECT * FROM height_with_prior_weight
),

calculated_bmi AS (
    -- Create calculated BMI records using the primary observation_id (height or weight)
    -- Only calculate BMI for dates without existing recorded BMI for the same person
    SELECT
        CASE 
            WHEN acp.calculation_source = 'weight_primary' THEN acp.weight_obs_id
            ELSE acp.height_obs_id
        END AS observation_id,
        acp.person_id,
        acp.clinical_effective_date,
        acp.calculated_bmi AS bmi_value,
        'kg/m²' AS result_unit_display,
        'CALCULATED_BMI' AS concept_code,
        'Calculated BMI from Height/Weight' AS concept_display,
        'CALCULATED' AS source_cluster_id,
        CAST(acp.calculated_bmi AS VARCHAR(20)) AS result_value,
        'calculated' AS bmi_source
    FROM all_calculated_pairs acp
    WHERE acp.calculated_bmi IS NOT NULL
      -- Avoid duplicates where same observation could be primary in both CTEs
      AND NOT EXISTS (
          SELECT 1 FROM recorded_bmi rb 
          WHERE rb.observation_id = CASE 
              WHEN acp.calculation_source = 'weight_primary' THEN acp.weight_obs_id
              ELSE acp.height_obs_id
          END
      )
      -- Only calculate BMI for dates without existing recorded BMI for same person
      AND NOT EXISTS (
          SELECT 1 FROM recorded_bmi rb 
          WHERE rb.person_id = acp.person_id
            AND rb.clinical_effective_date = acp.clinical_effective_date
      )
),

all_bmi AS (
    -- Combine recorded and calculated BMI
    SELECT * FROM recorded_bmi
    UNION ALL
    SELECT * FROM calculated_bmi
),

bmi_with_ethnicity AS (
    -- Join BMI data with ethnicity cardiometabolic risk information and age
    -- Filter to adults aged 18+ as pediatric BMI requires different percentile-based assessment
    SELECT
        ab.*,
        ecr.requires_lower_bmi_thresholds,
        ecr.cardiometabolic_risk_ethnicity_group,
        age.age
    FROM all_bmi ab
    INNER JOIN {{ ref('dim_person_age') }} age
        ON ab.person_id = age.person_id
        AND age.age >= 18  -- Adults only - pediatric BMI uses age/sex-specific percentiles
    LEFT JOIN {{ ref('int_ethnicity_cardiometabolic_risk') }} ecr
        ON ab.person_id = ecr.person_id
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

    -- Age information
    age,

    -- Ethnicity information
    requires_lower_bmi_thresholds,
    cardiometabolic_risk_ethnicity_group,

    -- Data quality validation
    CASE
        WHEN bmi_value BETWEEN 10 AND 150 THEN TRUE
        ELSE FALSE
    END AS is_valid_bmi,

    -- BMI categorisation (ethnicity-adjusted per NICE guidance)
    CASE
        WHEN bmi_value NOT BETWEEN 10 AND 150 THEN 'Invalid'
        WHEN bmi_value < 18.5 THEN 'Underweight'
        WHEN requires_lower_bmi_thresholds = TRUE THEN
            CASE
                WHEN bmi_value < 23 THEN 'Normal'
                WHEN bmi_value < 27.5 THEN 'Overweight'
                WHEN bmi_value < 32.5 THEN 'Obese Class I'
                WHEN bmi_value < 37.5 THEN 'Obese Class II'
                ELSE 'Obese Class III'
            END
        ELSE  -- Standard thresholds for other populations
            CASE
                WHEN bmi_value < 25 THEN 'Normal'
                WHEN bmi_value < 30 THEN 'Overweight'
                WHEN bmi_value < 35 THEN 'Obese Class I'
                WHEN bmi_value < 40 THEN 'Obese Class II'
                ELSE 'Obese Class III'
            END
    END AS bmi_category,

    -- BMI risk sort key (ethnicity-adjusted, higher number = higher risk)
    CASE
        WHEN bmi_value NOT BETWEEN 10 AND 150 THEN 0  -- Invalid
        WHEN bmi_value < 18.5 THEN 2  -- Underweight - Health risk
        WHEN requires_lower_bmi_thresholds = TRUE THEN
            CASE
                WHEN bmi_value < 23 THEN 1  -- Normal - Baseline/lowest risk
                WHEN bmi_value < 27.5 THEN 3  -- Overweight - Moderate risk
                WHEN bmi_value < 32.5 THEN 4  -- Obese Class I - High risk
                WHEN bmi_value < 37.5 THEN 5  -- Obese Class II - Higher risk
                ELSE 6  -- Obese Class III - Highest risk
            END
        ELSE  -- Standard thresholds for other populations
            CASE
                WHEN bmi_value < 25 THEN 1  -- Normal - Baseline/lowest risk
                WHEN bmi_value < 30 THEN 3  -- Overweight - Moderate risk
                WHEN bmi_value < 35 THEN 4  -- Obese Class I - High risk
                WHEN bmi_value < 40 THEN 5  -- Obese Class II - Higher risk
                ELSE 6  -- Obese Class III - Highest risk
            END
    END AS bmi_risk_sort_key

FROM bmi_with_ethnicity

-- Sort for consistent output
ORDER BY person_id, clinical_effective_date DESC
