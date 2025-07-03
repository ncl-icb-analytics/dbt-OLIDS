{{
    config(
        materialized='table',
        tags=['intermediate', 'ltc_lcs', 'hypertension'],
        cluster_by=['person_id', 'clinical_effective_date'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Intermediate: LTC LCS HTN Observations - Collects all hypertension-relevant observations for Long Term Conditions case finding measures including blood pressure measurements.

Clinical Purpose:
• Gathers comprehensive hypertension-related clinical observation data for case finding algorithms
• Supports identification of patients with undiagnosed hypertension through blood pressure measurements
• Enables observation-based risk stratification using clinic, home, and ABPM blood pressure readings
• Provides foundation data for hypertension case finding indicators with proper BP measurement pairing

Data Granularity:
• One row per clinical observation for hypertension-relevant observations
• Covers clinic, home, and ABPM blood pressure events with systolic/diastolic pairing
• Includes other hypertension-related observations: eGFR, BMI, BSA, cardiovascular complications
• Sourced from int_blood_pressure_all and LTC_LCS programme observation clusters

Key Features:
• Cluster IDs: HYPERTENSION_BP_CLINIC, HYPERTENSION_BP_HOME, HYPERTENSION_BP_ABPM, HYPERTENSION_EGFR, HYPERTENSION_BMI, HYPERTENSION_BSA, HYPERTENSION_MYOCARDIAL, HYPERTENSION_CEREBRAL, HYPERTENSION_CLAUDICATION, HYPERTENSION_DIABETES
• Proper blood pressure measurement pairing logic with observation type classification
• Comprehensive hypertension risk factor and diagnostic observation analysis
• Integration with existing blood pressure intermediate model and LTC_LCS observation clusters'"
        ]
    )
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
