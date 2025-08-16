{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

-- Hypertension Register (QOF Pattern 6: Complex Clinical Logic)
-- Business Logic: Age ≥18 + Active HTN diagnosis + Clinical staging based on latest BP with context-specific NICE thresholds
-- Complex Logic: BP staging varies by measurement context (Home/ABPM vs Clinic readings)

WITH hypertension_person_aggregates AS (
    SELECT
        person_id,

        -- Hypertension diagnosis dates
        MIN(
            CASE
                WHEN is_diagnosis_code THEN clinical_effective_date
            END
        ) AS earliest_diagnosis_date,
        MAX(
            CASE
                WHEN is_diagnosis_code THEN clinical_effective_date
            END
        ) AS latest_diagnosis_date,

        -- Resolution dates
        MIN(
            CASE
                WHEN is_resolved_code THEN clinical_effective_date
            END
        ) AS earliest_resolved_date,
        MAX(
            CASE
                WHEN is_resolved_code THEN clinical_effective_date
            END
        ) AS latest_resolved_date,

        -- Traceability arrays
        ARRAY_AGG(
            DISTINCT CASE
                WHEN is_diagnosis_code THEN concept_code
            END
        ) AS all_hypertension_concept_codes,
        ARRAY_AGG(
            DISTINCT CASE
                WHEN is_diagnosis_code THEN concept_display
            END
        ) AS all_hypertension_concept_displays,
        ARRAY_AGG(
            DISTINCT CASE
                WHEN is_resolved_code THEN concept_code
            END
        ) AS all_resolved_concept_codes,
        ARRAY_AGG(
            DISTINCT CASE
                WHEN is_resolved_code THEN concept_display
            END
        ) AS all_resolved_concept_displays

    FROM {{ ref('int_hypertension_diagnoses_all') }}
    GROUP BY person_id
),

-- Latest BP readings using new event-based structure
latest_bp_events AS (
    SELECT
        person_id,
        clinical_effective_date AS latest_bp_date,
        systolic_value AS latest_bp_systolic_value,
        diastolic_value AS latest_bp_diastolic_value,
        is_home_bp_event,
        is_abpm_bp_event,

        -- Context classification for threshold selection
        CASE
            WHEN is_abpm_bp_event THEN 'ABPM'
            WHEN is_home_bp_event THEN 'Home'
            ELSE 'Clinic'
        END AS bp_measurement_context
    FROM {{ ref('int_blood_pressure_latest') }}
    WHERE systolic_value IS NOT NULL AND diastolic_value IS NOT NULL
),

register_logic AS (
    SELECT
        p.person_id,

        -- Age restriction: ≥18 years for HTN register
        diag.earliest_diagnosis_date,

        -- QOF register logic: active hypertension diagnosis required
        diag.latest_diagnosis_date,

        -- Final register inclusion: Age + Active diagnosis required
        diag.latest_resolved_date,

        -- Clinical dates
        bp.latest_bp_date,
        bp.latest_bp_systolic_value,
        bp.latest_bp_diastolic_value,

        -- Latest BP data from event-based structure
        bp.bp_measurement_context,
        bp.is_home_bp_event,
        bp.is_abpm_bp_event,
        diag.all_hypertension_concept_codes,
        diag.all_hypertension_concept_displays,
        diag.all_resolved_concept_codes,

        -- NICE Guidelines: Context-specific BP staging with different thresholds
        diag.all_resolved_concept_displays,

        -- Traceability
        age.age,
        COALESCE(age.age >= 18, FALSE) AS meets_age_criteria,
        CASE
            WHEN diag.latest_resolved_date IS NULL THEN TRUE -- Never resolved
            WHEN diag.latest_diagnosis_date > diag.latest_resolved_date THEN TRUE -- Re-diagnosed after resolution
            ELSE FALSE -- Currently resolved
        END AS has_active_htn_diagnosis,
        COALESCE(
            age.age >= 18
            AND diag.earliest_diagnosis_date IS NOT NULL -- Has HTN diagnosis
            AND (
                diag.latest_resolved_date IS NULL -- Never resolved
                OR diag.latest_diagnosis_date > diag.latest_resolved_date -- Re-diagnosed after resolution
            ), FALSE
        ) AS is_on_register,

        -- Person demographics
        CASE
            WHEN
                bp.latest_bp_systolic_value IS NULL
                OR bp.latest_bp_diastolic_value IS NULL
                THEN NULL -- Cannot stage without paired BP values

            -- Severe hypertension (same threshold regardless of context)
            WHEN
                bp.latest_bp_systolic_value >= 180
                OR bp.latest_bp_diastolic_value >= 120
                THEN 'Severe HTN'

            -- Stage 2 Hypertension (context-specific thresholds)
            WHEN (
                bp.bp_measurement_context IN ('Home', 'ABPM')
                AND (
                    bp.latest_bp_systolic_value >= 155
                    OR bp.latest_bp_diastolic_value >= 95
                )
            )
            OR (
                bp.bp_measurement_context = 'Clinic'
                AND (
                    bp.latest_bp_systolic_value >= 160
                    OR bp.latest_bp_diastolic_value >= 100
                )
            )
                THEN 'Stage 2 HTN'

            -- Stage 1 Hypertension (context-specific thresholds)
            WHEN (
                bp.bp_measurement_context IN ('Home', 'ABPM')
                AND (
                    bp.latest_bp_systolic_value >= 135
                    OR bp.latest_bp_diastolic_value >= 85
                )
            )
            OR (
                bp.bp_measurement_context = 'Clinic'
                AND (
                    bp.latest_bp_systolic_value >= 140
                    OR bp.latest_bp_diastolic_value >= 90
                )
            )
                THEN 'Stage 1 HTN'

            ELSE 'Normal / High Normal'
        END AS latest_bp_htn_stage
    FROM {{ ref('dim_person') }} AS p
    INNER JOIN {{ ref('dim_person_age') }} AS age ON p.person_id = age.person_id
    LEFT JOIN
        hypertension_person_aggregates AS diag
        ON p.person_id = diag.person_id
    LEFT JOIN latest_bp_events AS bp ON p.person_id = bp.person_id
)

-- Final selection: Only individuals with active HTN diagnosis
SELECT
    person_id,
    age,
    is_on_register,

    -- Clinical diagnosis dates
    earliest_diagnosis_date,
    latest_diagnosis_date,
    latest_resolved_date,

    -- Latest BP data and staging
    latest_bp_date,
    latest_bp_systolic_value,
    latest_bp_diastolic_value,
    bp_measurement_context,
    is_home_bp_event,
    is_abpm_bp_event,
    latest_bp_htn_stage,

    -- Traceability for audit
    all_hypertension_concept_codes,
    all_hypertension_concept_displays,
    all_resolved_concept_codes,
    all_resolved_concept_displays,

    -- Criteria flags for transparency
    meets_age_criteria,
    has_active_htn_diagnosis
FROM register_logic
WHERE is_on_register = TRUE
