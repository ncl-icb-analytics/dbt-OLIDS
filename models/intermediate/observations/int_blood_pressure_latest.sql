{{
    config(
        materialized = 'table',
        tags = ['blood_pressure', 'latest'],
        cluster_by = ['person_id'])
}}

-- Latest Blood Pressure Event per person

SELECT
    person_id,
    clinical_effective_date,
    systolic_value,
    diastolic_value,
    is_home_bp_event,
    is_abpm_bp_event,
    -- Additional metadata for traceability
    result_unit_display,
    systolic_observation_id,
    diastolic_observation_id,
    all_concept_codes,
    all_concept_displays,
    all_source_cluster_ids
FROM {{ ref('int_blood_pressure_all') }}
QUALIFY
    ROW_NUMBER()
        OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC)
    = 1
