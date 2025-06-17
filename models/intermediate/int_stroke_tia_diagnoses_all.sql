{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

/*
All stroke and transient ischaemic attack (TIA) diagnoses from clinical records.
Uses QOF cluster ID STIA_COD for stroke and TIA diagnosis codes.

Clinical Purpose:
- Stroke/TIA register inclusion for QOF quality measures
- Stroke secondary prevention monitoring
- Cardiovascular risk management post-stroke

QOF Context:
Stroke/TIA register follows simple diagnosis-only pattern - any stroke or TIA diagnosis 
qualifies for register inclusion. No resolution codes or complex criteria.
This is a lifelong condition register for stroke secondary prevention.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Use this model as input for stroke/TIA register.
*/

WITH base_observations AS (
    SELECT
        person_id,
        clinical_effective_date,
        cluster_id AS source_cluster_id,
        mapped_concept_code AS concept_code,
        mapped_concept_display AS concept_description,
        result_text AS observation_value_text,
        result_value AS observation_value_numeric,
        result_value_unit_concept_id AS observation_units,
        observation_id  -- Using observation_id as substitute for date_recorded
    FROM (
        {{ get_observations("'STIA_COD'") }}
    ) o
),

-- Latest values first
latest_values AS (
    SELECT
        person_id,
        concept_code AS latest_stroke_tia_concept_code,
        concept_description AS latest_stroke_tia_concept_description,
        ROW_NUMBER() OVER (
            PARTITION BY person_id 
            ORDER BY clinical_effective_date DESC, observation_id DESC
        ) AS rn
    FROM base_observations
),

-- Person-level aggregation for efficient downstream use
person_level_aggregates AS (
    SELECT
        person_id,
        
        -- Diagnosis flags
        TRUE AS has_stroke_tia_diagnosis,
        
        -- Date aggregates
        MIN(clinical_effective_date) AS earliest_stroke_tia_date,
        MAX(clinical_effective_date) AS latest_stroke_tia_date,
        COUNT(DISTINCT clinical_effective_date) AS total_stroke_tia_episodes,
        
        -- Code arrays for detailed analysis
        ARRAY_AGG(DISTINCT concept_code) AS all_stroke_tia_concept_codes,
        ARRAY_AGG(DISTINCT concept_description) AS all_stroke_tia_concept_displays

    FROM base_observations
    GROUP BY person_id
)

SELECT
    pla.person_id,
    pla.has_stroke_tia_diagnosis,
    pla.earliest_stroke_tia_date,
    pla.latest_stroke_tia_date,
    pla.total_stroke_tia_episodes,
    pla.all_stroke_tia_concept_codes,
    pla.all_stroke_tia_concept_displays,
    lv.latest_stroke_tia_concept_code,
    lv.latest_stroke_tia_concept_description
FROM person_level_aggregates pla
LEFT JOIN latest_values lv 
    ON pla.person_id = lv.person_id 
    AND lv.rn = 1 