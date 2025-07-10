{{
    config(
        materialized='table',
        tags=['intermediate', 'clinical', 'blood_pressure'],
        cluster_by=['person_id', 'clinical_effective_date'])
}}

-- Blood Pressure Events - Event-based consolidation (one row per person per date)
-- Superior design for clinical analysis and BP control assessment

WITH base_observations_and_clusters AS (
    -- Get all BP-related observations with terminology mapping
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.result_value,
        'mmHg' AS result_unit_display, -- Default unit for BP readings
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id
    FROM ({{ get_observations("'BP_COD', 'SYSBP_COD', 'DIABP_COD', 'HOMEAMBBP_COD', 'ABPM_COD', 'HOMEBP_COD'") }}) obs
    WHERE obs.result_value IS NOT NULL
      AND obs.clinical_effective_date IS NOT NULL
      AND obs.clinical_effective_date <= CURRENT_DATE() -- No future dates
      -- Apply broad plausible value filter (refined later per BP type)
      AND obs.result_value > 20  -- Low threshold for either SBP or DBP
      AND obs.result_value < 350 -- High threshold applied broadly
),

row_flags AS (
    -- Determine BP type and clinical context for each observation
    SELECT
        *,
        -- Flag for Systolic readings: specific cluster or display text contains 'systolic'
        (source_cluster_id = 'SYSBP_COD' OR
         (source_cluster_id = 'BP_COD' AND concept_display ILIKE '%systolic%')) AS is_systolic_row,

        -- Flag for Diastolic readings: specific cluster or display text contains 'diastolic'
        (source_cluster_id = 'DIABP_COD' OR
         (source_cluster_id = 'BP_COD' AND concept_display ILIKE '%diastolic%')) AS is_diastolic_row,

        -- Flag for Home BP context
        (source_cluster_id IN ('HOMEBP_COD', 'HOMEAMBBP_COD')) AS is_home_bp_row,

        -- Flag for ABPM context
        (source_cluster_id = 'ABPM_COD') AS is_abpm_bp_row
    FROM base_observations_and_clusters
)

-- Event-level aggregation: one row per person per date with paired values
SELECT DISTINCT
    person_id,
    clinical_effective_date,

    -- Consolidated BP values: pivot systolic/diastolic for the event date
    MAX(CASE WHEN is_systolic_row THEN result_value ELSE NULL END) AS systolic_value,
    MAX(CASE WHEN is_diastolic_row THEN result_value ELSE NULL END) AS diastolic_value,

    -- Clinical context flags: if any observation on this date was Home/ABPM
    BOOLOR_AGG(is_home_bp_row) AS is_home_bp_event,
    BOOLOR_AGG(is_abpm_bp_row) AS is_abpm_bp_event,

    -- Traceability metadata for audit and debugging
    ANY_VALUE(result_unit_display) AS result_unit_display,
    MAX(CASE WHEN is_systolic_row THEN observation_id ELSE NULL END) AS systolic_observation_id,
    MAX(CASE WHEN is_diastolic_row THEN observation_id ELSE NULL END) AS diastolic_observation_id,
    ARRAY_AGG(DISTINCT concept_code) WITHIN GROUP (ORDER BY concept_code) AS all_concept_codes,
    ARRAY_AGG(DISTINCT concept_display) WITHIN GROUP (ORDER BY concept_display) AS all_concept_displays,
    ARRAY_AGG(DISTINCT source_cluster_id) WITHIN GROUP (ORDER BY source_cluster_id) AS all_source_cluster_ids

FROM row_flags
GROUP BY person_id, clinical_effective_date

-- Clinical validation: ensure we have valid BP events with plausible ranges
HAVING (systolic_value IS NOT NULL OR diastolic_value IS NOT NULL)
   AND (systolic_value IS NULL OR (systolic_value >= 40 AND systolic_value <= 350))
   AND (diastolic_value IS NULL OR (diastolic_value >= 20 AND diastolic_value <= 200))

ORDER BY person_id, clinical_effective_date DESC
