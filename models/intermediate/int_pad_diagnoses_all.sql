{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All peripheral arterial disease (PAD) diagnoses from clinical records.
Uses QOF cluster ID PAD_COD for all forms of PAD diagnosis.

Clinical Purpose:
- PAD register inclusion for QOF cardiovascular disease management
- Cardiovascular risk stratification and monitoring
- Secondary prevention pathway identification

QOF Context:
PAD register follows simple diagnosis-only pattern - any PAD diagnosis code 
qualifies for register inclusion. No resolution codes or complex criteria.
This is a lifelong condition register for cardiovascular secondary prevention.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Use this model as input for PAD register and cardiovascular risk models.
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
    FROM ({{ get_observations("'PAD_COD'") }}) obs
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
        TRUE AS has_pad_diagnosis,
        
        -- Date aggregates
        MIN(clinical_effective_date) AS earliest_pad_date,
        MAX(clinical_effective_date) AS latest_pad_date,
        COUNT(DISTINCT clinical_effective_date) AS total_pad_episodes,
        
        -- Recent episode indicators
        MAX(CASE WHEN clinical_effective_date >= DATEADD(month, -12, CURRENT_DATE()) THEN 1 ELSE 0 END) = 1 AS has_episode_last_12m,
        MAX(CASE WHEN clinical_effective_date >= DATEADD(month, -24, CURRENT_DATE()) THEN 1 ELSE 0 END) = 1 AS has_episode_last_24m,
        
        -- Code arrays for detailed analysis
        ARRAY_AGG(DISTINCT concept_code) AS all_pad_concept_codes,
        ARRAY_AGG(DISTINCT concept_description) AS all_pad_concept_displays,
        
        -- Latest values for reference
        FIRST_VALUE(concept_code) OVER (
            PARTITION BY person_id 
            ORDER BY clinical_effective_date DESC, date_recorded DESC
        ) AS latest_pad_concept_code,
        
        FIRST_VALUE(concept_description) OVER (
            PARTITION BY person_id 
            ORDER BY clinical_effective_date DESC, date_recorded DESC
        ) AS latest_pad_concept_description

    FROM observations_with_person
    GROUP BY person_id
)

SELECT
    person_id,
    has_pad_diagnosis,
    earliest_pad_date,
    latest_pad_date,
    total_pad_episodes,
    has_episode_last_12m,
    has_episode_last_24m,
    all_pad_concept_codes,
    all_pad_concept_displays,
    latest_pad_concept_code,
    latest_pad_concept_description
FROM person_level_aggregates 