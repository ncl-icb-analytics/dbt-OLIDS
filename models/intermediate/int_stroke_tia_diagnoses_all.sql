{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
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
        source_cluster_id,
        concept_code,
        concept_description,
        observation_value_text,
        observation_value_numeric,
        observation_units,
        date_recorded
    FROM {{ get_observations("'STIA_COD'") }}
),

-- Add person demographics for context
observations_with_person AS (
    SELECT
        obs.*,
        p.age_years,
        p.gender,
        p.is_active
    FROM base_observations obs
    LEFT JOIN {{ ref('dim_person') }} p
        ON obs.person_id = p.person_id
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
        ARRAY_AGG(DISTINCT concept_description) AS all_stroke_tia_concept_displays,
        
        -- Latest values for reference
        FIRST_VALUE(concept_code) OVER (
            PARTITION BY person_id 
            ORDER BY clinical_effective_date DESC, date_recorded DESC
        ) AS latest_stroke_tia_concept_code,
        
        FIRST_VALUE(concept_description) OVER (
            PARTITION BY person_id 
            ORDER BY clinical_effective_date DESC, date_recorded DESC
        ) AS latest_stroke_tia_concept_description

    FROM observations_with_person
    GROUP BY person_id
)

SELECT
    person_id,
    has_stroke_tia_diagnosis,
    earliest_stroke_tia_date,
    latest_stroke_tia_date,
    total_stroke_tia_episodes,
    all_stroke_tia_concept_codes,
    all_stroke_tia_concept_displays,
    latest_stroke_tia_concept_code,
    latest_stroke_tia_concept_description
FROM person_level_aggregates 