{{
    config(
        materialized='table',
        tags=['intermediate', 'ltc_lcs', 'hypertension'],
        cluster_by=['person_id', 'clinical_effective_date'])
}}

-- Hypertension observations for LTC/LCS case finding
-- Combines blood pressure events with other hypertension-related observations

WITH blood_pressure_events AS (
    -- Use the existing blood pressure intermediate with proper pairing logic
    SELECT
        person_id,
        clinical_effective_date,
        systolic_value,
        diastolic_value,
        is_home_bp_event,
        is_abpm_bp_event,
        -- Determine BP type for case finding logic
        CASE
            WHEN is_abpm_bp_event THEN 'HYPERTENSION_BP_ABPM'
            WHEN is_home_bp_event THEN 'HYPERTENSION_BP_HOME'
            ELSE 'HYPERTENSION_BP_CLINIC'
        END AS cluster_id,
        all_concept_codes AS mapped_concept_codes,
        all_concept_displays AS mapped_concept_displays
    FROM {{ ref('int_blood_pressure_all') }}
),

other_htn_observations AS (
    -- Get other hypertension-related observations from clusters
    {{ get_observations(
        cluster_ids="'HYPERTENSION_EGFR', 'HYPERTENSION_BMI', 'HYPERTENSION_BSA', 'HYPERTENSION_MYOCARDIAL', 'HYPERTENSION_CEREBRAL', 'HYPERTENSION_CLAUDICATION', 'HYPERTENSION_DIABETES'",
        source='LTC_LCS'
    ) }}
),

combined_observations AS (
    -- Blood pressure events
    SELECT
        person_id,
        clinical_effective_date,
        cluster_id,
        systolic_value AS result_value,
        mapped_concept_codes,
        mapped_concept_displays,
        'BP_SYSTOLIC' AS observation_type
    FROM blood_pressure_events
    WHERE systolic_value IS NOT NULL

    UNION ALL

    SELECT
        person_id,
        clinical_effective_date,
        cluster_id,
        diastolic_value AS result_value,
        mapped_concept_codes,
        mapped_concept_displays,
        'BP_DIASTOLIC' AS observation_type
    FROM blood_pressure_events
    WHERE diastolic_value IS NOT NULL

    UNION ALL

    -- Other hypertension observations
    SELECT
        person_id,
        clinical_effective_date,
        cluster_id,
        result_value,
        ARRAY_CONSTRUCT(mapped_concept_code) AS mapped_concept_codes,
        ARRAY_CONSTRUCT(mapped_concept_display) AS mapped_concept_displays,
        'OTHER' AS observation_type
    FROM other_htn_observations
    WHERE clinical_effective_date IS NOT NULL
)

SELECT
    person_id,
    clinical_effective_date,
    cluster_id,
    result_value,
    mapped_concept_codes,
    mapped_concept_displays,
    observation_type
FROM combined_observations
ORDER BY person_id, clinical_effective_date DESC
